"""
Flask backend for XER → Smartsheet import tool.
Endpoints:
  POST /api/import   - Upload XER, push to Smartsheet
  GET  /health       - Health check
"""

import os
import smartsheet
from flask import Flask, request, jsonify
from flask_cors import CORS
from xer_parser import parse_xer

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
    activities = parsed["activities"]

    if not activities:
        return jsonify({"error": "No activities found in XER file"}), 422

    try:
        sheet_url = _push_to_smartsheet(api_key, project_name, activities)
    except Exception as e:
        return jsonify({"error": f"Smartsheet error: {str(e)}"}), 500

    return jsonify({
        "success": True,
        "sheet_name": project_name,
        "activity_count": len(activities),
        "sheet_url": sheet_url,
    })


def _push_to_smartsheet(api_key: str, sheet_name: str, activities: list) -> str:
    """
    Create or overwrite a Smartsheet with activities.
    Returns the permalink URL of the sheet.
    """
    ss = smartsheet.Smartsheet(api_key)
    ss.errors_as_exceptions(True)

    # --- Find or create sheet ---
    existing_id = None
    sheets = ss.Sheets.list_sheets(include_all=True)
    for s in sheets.data:
        if s.name == sheet_name:
            existing_id = s.id
            break

    column_defs = [
        {"title": "Task Name",    "type": "TEXT_NUMBER", "primary": True},
        {"title": "WBS",          "type": "TEXT_NUMBER"},
        {"title": "Start",        "type": "DATE"},
        {"title": "Finish",       "type": "DATE"},
        {"title": "Duration",     "type": "TEXT_NUMBER"},
        {"title": "Predecessors", "type": "TEXT_NUMBER"},
        {"title": "Assigned To",  "type": "TEXT_NUMBER"},
    ]

    if existing_id:
        # Delete all existing rows to reset the sheet (batch max 450 per API call)
        sheet = ss.Sheets.get_sheet(existing_id)
        if sheet.rows:
            row_ids = [r.id for r in sheet.rows]
            for i in range(0, len(row_ids), 450):
                ss.Sheets.delete_rows(existing_id, row_ids[i:i + 450])
        sheet_id = existing_id
        col_map = {c.title: c.id for c in sheet.columns}
        # Add any columns that exist in our definition but are missing from the sheet
        for col_def in column_defs:
            if col_def["title"] not in col_map and not col_def.get("primary"):
                col_obj = smartsheet.models.Column(
                    {"title": col_def["title"], "type": col_def["type"]}
                )
                added = ss.Sheets.add_columns(sheet_id, [col_obj])
                col_map[col_def["title"]] = added.result[0].id
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

    # Enable Smartsheet dependency engine so predecessors drive start dates
      try:
        _enable_dependencies(ss, sheet_id, col_map)
    except Exception as dep_err:
        print(f"Warning: could not enable dependencies: {dep_err}"


    # --- Build rows (batch in groups of 500) ---
    def make_row(act):
        row = smartsheet.models.Row()
        row.to_bottom = True
        dur = act.get("duration", 0)
        fields = {
            "Task Name":    act.get("task_name", ""),
            "WBS":          act.get("wbs", ""),
            "Start":        act.get("start", ""),
            "Finish":       act.get("finish", ""),
            "Duration":     int(dur) if dur == int(dur) else dur,
            "Predecessors": act.get("predecessors", ""),
            "Assigned To":  act.get("assigned_to", ""),
        }

        for col_name, value in fields.items():
            if col_name in col_map and value is not None:
                cell = smartsheet.models.Cell()
                cell.column_id = col_map[col_name]
                cell.value = value
                row.cells.append(cell)
        return row

    batch_size = 500
    for i in range(0, len(activities), batch_size):
        batch = activities[i:i + batch_size]
        rows = [r for r in (make_row(a) for a in batch) if r.cells]
        if rows:
            ss.Sheets.add_rows(sheet_id, rows)

    # Return permalink
    sheet_info = ss.Sheets.get_sheet(sheet_id)
    return sheet_info.permalink

def _enable_dependencies(ss, sheet_id: int, col_map: dict):
    """
    Enable Smartsheet Gantt dependency engine.
    Once enabled, the Predecessors column drives start date calculations:
    each task start date is set to the latest finish date of its predecessors.
    """
    project_settings = smartsheet.models.ProjectSettings({
        "workingDays": ["MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY"],
        "nonWorkingDays": [],
        "lengthOfDay": 8,
        "useWorkingDays": True,
    })
    sheet_update = smartsheet.models.Sheet({
        "dependenciesEnabled": True,
        "projectSettings": project_settings,
    })
    ss.Sheets.update_sheet(sheet_id, sheet_update)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
