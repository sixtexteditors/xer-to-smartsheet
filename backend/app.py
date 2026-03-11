"""
Flask backend for XER → Smartsheet import tool.
Endpoints:
  POST /api/import   - Upload XER, push to Smartsheet
  GET  /health       - Health check
"""

import os
import time
import random
import traceback
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import smartsheet
import smartsheet.exceptions
from flask import Flask, request, jsonify
from flask_cors import CORS
from xer_parser import parse_xer, _lag_to_days, _normalize_rel_type

# In Docker, frontend is at /app/frontend (sibling of this file).
# In local dev (running from backend/), frontend is at ../frontend.
_here = os.path.dirname(os.path.abspath(__file__))
_frontend = os.path.join(_here, "frontend")
if not os.path.isdir(_frontend):
    _frontend = os.path.join(_here, "..", "frontend")

app = Flask(__name__, static_folder=_frontend, static_url_path="")
CORS(app)

app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB upload limit


def _with_retry(fn, max_attempts=6):
    """Call fn(), retrying on Smartsheet rate limit or transient errors with exponential backoff."""
    for attempt in range(max_attempts):
        try:
            return fn()
        except smartsheet.exceptions.ApiError as e:
            try:
                code = e.error.result.code
            except Exception:
                code = None
            # 1063 = invalid parentId (eventual consistency), 4003 = rate limit, 4004 = timeout
            if code in (1063, 4003, 4004) and attempt < max_attempts - 1:
                wait = (2 ** attempt) + random.uniform(0, 1)
                time.sleep(wait)
            else:
                raise
        except Exception:
            if attempt < max_attempts - 1:
                time.sleep(1 + random.uniform(0, 0.5))
            else:
                raise


@app.route("/")
def index():
    return app.send_static_file("index.html")


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


@app.route("/api/import", methods=["POST"])
def import_xer():
    api_key = request.headers.get("X-Smartsheet-Token", "").strip()
    if not api_key:
        return jsonify({"error": "Missing Smartsheet API key"}), 401

    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    xer_file = request.files["file"]
    try:
        content = xer_file.read().decode("utf-8", errors="replace")
    except Exception as e:
        return jsonify({"error": f"Could not read file: {str(e)}"}), 400

    try:
        parsed = parse_xer(content)
    except Exception as e:
        return jsonify({"error": f"XER parse error: {str(e)}"}), 422

    project_name = request.form.get("sheet_name", "").strip() or parsed["project_name"]
    activities_flat = parsed["activities_flat"]

    if not activities_flat:
        return jsonify({"error": "No activities found in XER file"}), 422

    try:
        sheet_url = _push_to_smartsheet(api_key, project_name, parsed)
    except smartsheet.exceptions.ApiError as e:
        try:
            result = e.error.result
            err = f"{result.code}: {result.message}"
        except Exception:
            err = str(e)
        return jsonify({"error": err}), 500
    except Exception as e:
        return jsonify({"error": str(e), "detail": traceback.format_exc()}), 500

    return jsonify({
        "success": True,
        "sheet_name": project_name,
        "activity_count": len(activities_flat),
        "sheet_url": sheet_url,
    })


def _push_to_smartsheet(api_key: str, sheet_name: str, parsed: dict) -> str:
    """
    Create or overwrite a Smartsheet with WBS hierarchy and activities.

    Flow:
      1. Find or create sheet
      2. Insert WBS rows in parallel by depth
      3. Insert all activities flat (no parent) in sequential batches
      4. Pass 2a: update_rows to place each activity under its WBS parent
      5. Pass 2b: fetch sheet for row numbers, update predecessor/dependent columns

    Why flat insert + separate hierarchy pass:
      add_rows requires all rows in one call to share the same parent_id.
      With 800 unique WBS parents this meant 800 parallel calls, causing 1063
      (invalid parentId) errors due to Smartsheet eventual consistency.
      update_rows allows mixed parent_ids per batch (unlike add_rows),
      so 3,781 activities need only ~8 API calls instead of 800.

    Returns the permalink URL of the sheet.
    """
    ss = smartsheet.Smartsheet(api_key)
    ss.errors_as_exceptions(True)

    wbs_tree = parsed["wbs_tree"]
    activities_flat = parsed["activities_flat"]
    predecessor_map = parsed["predecessor_map"]

    # -------------------------------------------------------------------------
    # Find or create sheet
    # -------------------------------------------------------------------------
    existing_id = None
    sheets = ss.Sheets.list_sheets(include_all=True)
    for s in sheets.data:
        if s.name == sheet_name:
            existing_id = s.id
            break

    column_defs = [
        {"title": "Task Name",             "type": "TEXT_NUMBER", "primary": True},
        {"title": "Start",                 "type": "DATE"},
        {"title": "Finish",                "type": "DATE"},
        {"title": "Duration",              "type": "TEXT_NUMBER"},
        {"title": "Predecessors",          "type": "TEXT_NUMBER"},
        {"title": "Assigned To",           "type": "TEXT_NUMBER"},
        {"title": "Facility",              "type": "TEXT_NUMBER"},
        {"title": "Activity Type",         "type": "TEXT_NUMBER"},
        {"title": "Activity ID",           "type": "TEXT_NUMBER"},
        {"title": "Predecessor Names",     "type": "TEXT_NUMBER"},
        {"title": "Dependent Row Numbers", "type": "TEXT_NUMBER"},
        {"title": "Dependent Names",       "type": "TEXT_NUMBER"},
    ]

    if existing_id:
        sheet = ss.Sheets.get_sheet(existing_id)
        if sheet.rows:
            root_ids = [r.id for r in sheet.rows if not getattr(r, "parent_id", None)]
            for i in range(0, len(root_ids), 450):
                ss.Sheets.delete_rows(existing_id, root_ids[i:i + 450])
        sheet_id = existing_id
        col_map = {c.title: c.id for c in sheet.columns}
        for col_def in column_defs:
            if col_def["title"] not in col_map and not col_def.get("primary"):
                col_obj = smartsheet.models.Column(
                    {"title": col_def["title"], "type": col_def["type"]}
                )
                added = ss.Sheets.add_columns(sheet_id, [col_obj])
                added_cols = added.result if isinstance(added.result, list) else [added.result]
                col_map[col_def["title"]] = added_cols[0].id
    else:
        cols = [smartsheet.models.Column({"title": c["title"], "type": c["type"],
                                          "primary": c.get("primary", False)})
                for c in column_defs]
        new_sheet = smartsheet.models.Sheet({"name": sheet_name, "columns": cols})
        result = ss.Home.create_sheet(new_sheet)
        sheet_id = result.result.id
        sheet = ss.Sheets.get_sheet(sheet_id)
        col_map = {c.title: c.id for c in sheet.columns}

    batch_size = 500

    def make_cell(col_name, value):
        cell = smartsheet.models.Cell()
        cell.column_id = col_map[col_name]
        cell.value = value
        return cell

    def _build_activity_row(act):
        """Build a flat Row for an activity (no parent_id — hierarchy set in Pass 2a)."""
        row = smartsheet.models.Row()
        row.to_bottom = True
        cells = [make_cell("Task Name", act.get("task_name", ""))]
        if act.get("start"):
            cells.append(make_cell("Start", act["start"]))
        if act.get("finish"):
            cells.append(make_cell("Finish", act["finish"]))
        cells.append(make_cell("Duration", str(act.get("duration", ""))))
        if act.get("assigned_to"):
            cells.append(make_cell("Assigned To", act["assigned_to"]))
        if act.get("facility"):
            cells.append(make_cell("Facility", act["facility"]))
        if act.get("activity_type"):
            cells.append(make_cell("Activity Type", act["activity_type"]))
        if act.get("activity_id"):
            cells.append(make_cell("Activity ID", act["activity_id"]))
        row.cells = cells
        return row

    # -------------------------------------------------------------------------
    # PASS 1a: Insert WBS rows in parallel, depth by depth
    # Depths are processed sequentially because each level depends on parent
    # SS row IDs from the level above. Groups within a depth run in parallel.
    # -------------------------------------------------------------------------
    wbs_id_to_ss_row_id = {}

    wbs_by_depth = defaultdict(list)
    for node in wbs_tree:
        wbs_by_depth[node["depth"]].append(node)

    def _insert_wbs_group(parent_wbs_id, siblings):
        ss_local = smartsheet.Smartsheet(api_key)
        ss_local.errors_as_exceptions(True)
        parent_ss_id = wbs_id_to_ss_row_id.get(parent_wbs_id) if parent_wbs_id else None
        group_results = []
        for i in range(0, len(siblings), batch_size):
            batch = siblings[i:i + batch_size]
            rows = []
            for node in batch:
                wbs_row = smartsheet.models.Row()
                wbs_row.to_bottom = True
                if parent_ss_id:
                    wbs_row.parent_id = parent_ss_id
                wbs_row.cells = [make_cell("Task Name", node["wbs_name"])]
                rows.append((node["wbs_id"], wbs_row))
            _batch_rows = [r for _, r in rows]
            result = _with_retry(lambda r=_batch_rows: ss_local.Sheets.add_rows(sheet_id, r))
            returned = result.result if isinstance(result.result, list) else [result.result]
            for (wbs_id, _), returned_row in zip(rows, returned):
                group_results.append((wbs_id, returned_row.id))
        return group_results

    for depth in sorted(wbs_by_depth.keys()):
        by_parent = defaultdict(list)
        for node in wbs_by_depth[depth]:
            by_parent[node["parent_wbs_id"]].append(node)

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(_insert_wbs_group, parent_wbs_id, siblings): parent_wbs_id
                for parent_wbs_id, siblings in by_parent.items()
            }
            errors = []
            for future in as_completed(futures):
                try:
                    for wbs_id, ss_row_id in future.result():
                        wbs_id_to_ss_row_id[wbs_id] = ss_row_id
                except Exception as e:
                    errors.append(str(e))
            if errors:
                raise Exception(f"WBS insertion failed: {errors[0]}")

    # -------------------------------------------------------------------------
    # PASS 1b: Insert all activities flat (no parent_id)
    # Brief sleep first to let WBS rows fully commit on Smartsheet's side.
    # -------------------------------------------------------------------------
    time.sleep(3)

    task_id_to_ss_row_id = {}
    for i in range(0, len(activities_flat), batch_size):
        batch = activities_flat[i:i + batch_size]
        rows = [(act["_task_id"], _build_activity_row(act)) for act in batch]
        _batch_rows = [r for _, r in rows]
        result = _with_retry(lambda r=_batch_rows: ss.Sheets.add_rows(sheet_id, r))
        returned = result.result if isinstance(result.result, list) else [result.result]
        for (task_id, _), returned_row in zip(rows, returned):
            task_id_to_ss_row_id[task_id] = returned_row.id

    # -------------------------------------------------------------------------
    # PASS 2a: Place each activity under its WBS parent via update_rows
    # update_rows allows mixed parent_ids per batch (unlike add_rows),
    # so 3,781 activities need only ~8 API calls instead of 800.
    # -------------------------------------------------------------------------
    parent_update_rows = []
    for act in activities_flat:
        tid = act["_task_id"]
        act_ss_row_id = task_id_to_ss_row_id.get(tid)
        if act_ss_row_id is None:
            continue
        parent_ss_id = wbs_id_to_ss_row_id.get(act["_wbs_id"])
        if parent_ss_id is None:
            continue
        update_row = smartsheet.models.Row()
        update_row.id = act_ss_row_id
        update_row.parent_id = parent_ss_id
        parent_update_rows.append(update_row)

    for i in range(0, len(parent_update_rows), batch_size):
        _with_retry(lambda b=parent_update_rows[i:i + batch_size]: ss.Sheets.update_rows(sheet_id, b))

    # -------------------------------------------------------------------------
    # PASS 2b: Update predecessor, predecessor names, dependent, dependent name columns
    # Fetch the sheet first to get accurate row numbers after hierarchy placement.
    # -------------------------------------------------------------------------
    task_id_to_name = {act["_task_id"]: act["task_name"] for act in activities_flat}

    dependent_map = defaultdict(list)
    for task_id, preds in predecessor_map.items():
        for p in preds:
            dependent_map[p["pred_task_id"]].append(task_id)

    sheet_data = ss.Sheets.get_sheet(sheet_id)
    ss_row_id_to_row_number = {r.id: r.row_number for r in (sheet_data.rows or [])}

    update_rows = []
    for act in activities_flat:
        tid = act["_task_id"]
        act_ss_row_id = task_id_to_ss_row_id.get(tid)
        if act_ss_row_id is None:
            continue

        cells_to_update = []

        # Predecessors (row numbers + names)
        preds = predecessor_map.get(tid, [])
        pred_parts = []
        pred_name_parts = []
        for p in preds:
            pred_tid = p["pred_task_id"]
            pred_ss_row_id = task_id_to_ss_row_id.get(pred_tid)
            if pred_ss_row_id is None:
                continue
            row_number = ss_row_id_to_row_number.get(pred_ss_row_id)
            if row_number is None:
                continue
            lag_days = _lag_to_days(p.get("lag_hr_cnt", "0"))
            rel_type = _normalize_rel_type(p.get("pred_type", "PR_FS"))
            if lag_days != 0:
                sign = "+" if lag_days > 0 else ""
                pred_parts.append(f"{row_number}{rel_type}{sign}{lag_days}d")
            elif rel_type != "FS":
                pred_parts.append(f"{row_number}{rel_type}")
            else:
                pred_parts.append(str(row_number))
            pred_name = task_id_to_name.get(pred_tid, "")
            if pred_name:
                pred_name_parts.append(pred_name)

        if pred_parts:
            cell = smartsheet.models.Cell()
            cell.column_id = col_map["Predecessors"]
            cell.value = ",".join(pred_parts)
            cells_to_update.append(cell)

        if pred_name_parts:
            cell = smartsheet.models.Cell()
            cell.column_id = col_map["Predecessor Names"]
            cell.value = ", ".join(pred_name_parts)
            cells_to_update.append(cell)

        # Dependents (row numbers + names)
        dep_tids = dependent_map.get(tid, [])
        dep_parts = []
        dep_name_parts = []
        for dep_tid in dep_tids:
            dep_ss_row_id = task_id_to_ss_row_id.get(dep_tid)
            if dep_ss_row_id is None:
                continue
            row_number = ss_row_id_to_row_number.get(dep_ss_row_id)
            if row_number is None:
                continue
            dep_parts.append(str(row_number))
            dep_name = task_id_to_name.get(dep_tid, "")
            if dep_name:
                dep_name_parts.append(dep_name)

        if dep_parts:
            cell = smartsheet.models.Cell()
            cell.column_id = col_map["Dependent Row Numbers"]
            cell.value = ",".join(dep_parts)
            cells_to_update.append(cell)

        if dep_name_parts:
            cell = smartsheet.models.Cell()
            cell.column_id = col_map["Dependent Names"]
            cell.value = ", ".join(dep_name_parts)
            cells_to_update.append(cell)

        if cells_to_update:
            update_row = smartsheet.models.Row()
            update_row.id = act_ss_row_id
            update_row.cells = cells_to_update
            update_rows.append(update_row)

    for i in range(0, len(update_rows), batch_size):
        _with_retry(lambda b=update_rows[i:i + batch_size]: ss.Sheets.update_rows(sheet_id, b))

    return sheet_data.permalink


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
