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
            # 4003 = rate limit exceeded, 4004 = server timeout
            if code in (4003, 4004) and attempt < max_attempts - 1:
                wait = (2 ** attempt) + random.uniform(0, 1)
                time.sleep(wait)
            else:
                raise
        except Exception:
            if attempt < max_attempts - 1:
                time.sleep(1 + random.uniform(0, 0.5))
            else:
                raise

# In Docker, frontend is at /app/frontend (sibling of this file).
# In local dev (running from backend/), frontend is at ../frontend.
_here = os.path.dirname(os.path.abspath(__file__))
_frontend = os.path.join(_here, "frontend")
if not os.path.isdir(_frontend):
    _frontend = os.path.join(_here, "..", "frontend")

app = Flask(__name__, static_folder=_frontend, static_url_path="")
CORS(app)

app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB upload limit


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
    Uses a two-pass approach: insert all rows first, then update predecessor
    cells with correct Smartsheet row numbers.
    Returns the permalink URL of the sheet.
    """
    ss = smartsheet.Smartsheet(api_key)
    ss.errors_as_exceptions(True)

    wbs_tree = parsed["wbs_tree"]
    activities_by_wbs = parsed["activities_by_wbs"]
    activities_flat = parsed["activities_flat"]
    predecessor_map = parsed["predecessor_map"]

    # --- Find or create sheet ---
    existing_id = None
    sheets = ss.Sheets.list_sheets(include_all=True)
    for s in sheets.data:
        if s.name == sheet_name:
            existing_id = s.id
            break

    column_defs = [
        {"title": "Task Name",    "type": "TEXT_NUMBER", "primary": True},
        {"title": "Start",        "type": "DATE"},
        {"title": "Finish",       "type": "DATE"},
        {"title": "Duration",     "type": "TEXT_NUMBER"},
        {"title": "Predecessors", "type": "TEXT_NUMBER"},
        {"title": "Assigned To",    "type": "TEXT_NUMBER"},
        {"title": "Facility",        "type": "TEXT_NUMBER"},
        {"title": "Activity Type",   "type": "TEXT_NUMBER"},
        {"title": "Activity ID",           "type": "TEXT_NUMBER"},
        {"title": "Predecessor Names",     "type": "TEXT_NUMBER"},
        {"title": "Dependent Row Numbers", "type": "TEXT_NUMBER"},
        {"title": "Dependent Names",       "type": "TEXT_NUMBER"},
    ]

    if existing_id:
        # Delete only root-level rows — Smartsheet cascades to children automatically.
        # Passing child row IDs after their parent is already deleted causes API errors.
        sheet = ss.Sheets.get_sheet(existing_id)
        if sheet.rows:
            root_ids = [r.id for r in sheet.rows if not getattr(r, "parent_id", None)]
            for i in range(0, len(root_ids), 450):
                ss.Sheets.delete_rows(existing_id, root_ids[i:i + 450])
        sheet_id = existing_id
        col_map = {c.title: c.id for c in sheet.columns}
        # Add any columns that exist in our definition but are missing from the sheet
        for col_def in column_defs:
            if col_def["title"] not in col_map and not col_def.get("primary"):
                col_obj = smartsheet.models.Column(
                    {"title": col_def["title"], "type": col_def["type"]}
                )
                added = ss.Sheets.add_columns(sheet_id, [col_obj])
                added_cols = added.result if isinstance(added.result, list) else [added.result]
                col_map[col_def["title"]] = added_cols[0].id
    else:
        # Create new sheet
        cols = [smartsheet.models.Column({"title": c["title"], "type": c["type"],
                                          "primary": c.get("primary", False)})
                for c in column_defs]
        new_sheet = smartsheet.models.Sheet({"name": sheet_name, "columns": cols})
        result = ss.Home.create_sheet(new_sheet)
        sheet_id = result.result.id
        sheet = ss.Sheets.get_sheet(sheet_id)
        col_map = {c.title: c.id for c in sheet.columns}

    # --- PASS 1: Insert rows with WBS hierarchy ---
    # Strategy: insert WBS nodes level-by-level (1 batch call per depth), then
    # insert ALL activities in a single global pass (1 batch call per 500 rows).
    # This is far fewer API calls than inserting per-node for large schedules.
    wbs_id_to_ss_row_id = {}   # wbs_id -> Smartsheet row ID
    task_id_to_ss_row_id = {}  # XER task_id -> Smartsheet row ID
    batch_size = 500

    def make_cell(col_name, value):
        cell = smartsheet.models.Cell()
        cell.column_id = col_map[col_name]
        cell.value = value
        return cell

    def _build_activity_row(act, parent_ss_id=None):
        row = smartsheet.models.Row()
        row.to_bottom = True
        if parent_ss_id:
            row.parent_id = parent_ss_id
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

    # Smartsheet requires all rows in one add_rows call to share the same
    # parent_id value. Group by parent before batching.

    # WBS nodes: group by depth, then by parent within each depth.
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
            _rows = [r for _, r in rows]
            result = _with_retry(lambda r=_rows: ss_local.Sheets.add_rows(sheet_id, r))
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
            for future in as_completed(futures):
                for wbs_id, ss_row_id in future.result():
                    wbs_id_to_ss_row_id[wbs_id] = ss_row_id

    # Activities: group by WBS parent so each batch shares the same parent_id.
    by_parent_ss = defaultdict(list)
    for act in activities_flat:
        by_parent_ss[wbs_id_to_ss_row_id.get(act["_wbs_id"])].append(act)

    def _insert_activity_group(parent_ss_id, acts):
        ss_local = smartsheet.Smartsheet(api_key)
        ss_local.errors_as_exceptions(True)
        group_results = []
        for i in range(0, len(acts), batch_size):
            batch = acts[i:i + batch_size]
            rows = [(act["_task_id"], _build_activity_row(act, parent_ss_id)) for act in batch]
            _rows = [r for _, r in rows]
            result = _with_retry(lambda r=_rows: ss_local.Sheets.add_rows(sheet_id, r))
            returned = result.result if isinstance(result.result, list) else [result.result]
            for (task_id, _), returned_row in zip(rows, returned):
                group_results.append((task_id, returned_row.id))
        return group_results

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(_insert_activity_group, parent_ss_id, acts): parent_ss_id
            for parent_ss_id, acts in by_parent_ss.items()
        }
        for future in as_completed(futures):
            for task_id, ss_row_id in future.result():
                task_id_to_ss_row_id[task_id] = ss_row_id

    # Build task_id -> task_name lookup for predecessor/dependent name columns
    task_id_to_name = {act["_task_id"]: act["task_name"] for act in activities_flat}

    # Build dependent_map: task_id -> list of task_ids that depend on it (inverse of predecessor_map)
    dependent_map = defaultdict(list)
    for task_id, preds in predecessor_map.items():
        for p in preds:
            dependent_map[p["pred_task_id"]].append(task_id)

    # --- PASS 2: Update predecessors with actual Smartsheet row numbers ---
    sheet_data = ss.Sheets.get_sheet(sheet_id)
    ss_row_id_to_row_number = {r.id: r.row_number for r in (sheet_data.rows or [])}

    update_rows = []
    for act in activities_flat:
        tid = act["_task_id"]
        act_ss_row_id = task_id_to_ss_row_id.get(tid)
        if act_ss_row_id is None:
            continue

        cells_to_update = []

        # Predecessor row numbers (existing column)
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

        # Dependent row numbers and names
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

    batch_size = 500
    update_batches = [update_rows[i:i + batch_size] for i in range(0, len(update_rows), batch_size)]

    def _send_update_batch(batch):
        ss_local = smartsheet.Smartsheet(api_key)
        ss_local.errors_as_exceptions(True)
        return _with_retry(lambda: ss_local.Sheets.update_rows(sheet_id, batch))

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(_send_update_batch, batch) for batch in update_batches]
        for future in as_completed(futures):
            future.result()

    # sheet_data was fetched at the start of Pass 2 and already has permalink
    return sheet_data.permalink



if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
