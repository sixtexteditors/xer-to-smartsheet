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

# ID of the Smartsheet template that has Dependencies Enabled and the correct
# columns pre-configured. New sheets are created from this template so they
# inherit those settings without needing any extra API calls.
TEMPLATE_SHEET_ID = 2502405129195396

# Resolve the frontend folder — works both in Docker and local dev
_here = os.path.dirname(os.path.abspath(__file__))
_frontend = os.path.join(_here, "frontend")
if not os.path.isdir(_frontend):
    _frontend = os.path.join(_here, "..", "frontend")

# Create Flask app and point it at the frontend folder for static file serving
app = Flask(__name__, static_folder=_frontend, static_url_path="")
CORS(app)  # Allow cross-origin requests (needed when frontend/backend are on different ports)

app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # Reject uploads larger than 50 MB


@app.route("/")
def index():
    # Serve the single-page frontend UI
    return app.send_static_file("index.html")


@app.route("/health")
def health():
    # Simple liveness check used by Railway to confirm the app is running
    return jsonify({"status": "ok"})


@app.route("/api/import", methods=["POST"])
def import_xer():
    # Pull the Smartsheet API key from the request header
    api_key = request.headers.get("X-Smartsheet-Token", "").strip()
    if not api_key:
        return jsonify({"error": "Missing Smartsheet API key"}), 401

    # Confirm a file was included in the form submission
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    xer_file = request.files["file"]
    try:
        # Read and decode the uploaded XER file as text
        content = xer_file.read().decode("utf-8", errors="replace")
    except Exception as e:
        return jsonify({"error": f"Could not read file: {str(e)}"}), 400

    try:
        # Parse the XER file into a structured dict of activities
        parsed = parse_xer(content)
    except Exception as e:
        return jsonify({"error": f"XER parse error: {str(e)}"}), 422

    # Use the user-supplied sheet name if given, otherwise fall back to the project name in the XER
    project_name = request.form.get("sheet_name", "").strip() or parsed["project_name"]
    activities = parsed["activities"]

    if not activities:
        return jsonify({"error": "No activities found in XER file"}), 422

    try:
        # Push everything to Smartsheet and get back the sheet URL
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
    Create or overwrite a Smartsheet with the imported activities.
    New sheets are created from TEMPLATE_SHEET_ID so Dependencies Enabled
    and column configuration are inherited automatically.
    Returns the permalink URL of the resulting sheet.
    """
    # Initialise the Smartsheet SDK client with the user's API key
    ss = smartsheet.Smartsheet(api_key)
    ss.errors_as_exceptions(True)  # Raise exceptions instead of returning error objects

    # Search the user's sheets for one that already has the same name
    existing_id = None
    sheets = ss.Sheets.list_sheets(include_all=True)
    for s in sheets.data:
        if s.name == sheet_name:
            existing_id = s.id
            break

    if existing_id:
        # Sheet exists — fetch it and delete all its rows so we can re-import fresh
        sheet = ss.Sheets.get_sheet(existing_id)
        if sheet.rows:
            row_ids = [r.id for r in sheet.rows]
            # Smartsheet limits bulk deletes to 450 rows per call
            for i in range(0, len(row_ids), 450):
                ss.Sheets.delete_rows(existing_id, row_ids[i:i + 450])
        sheet_id = existing_id
        # Build a map of column name → column ID for populating cells later
        col_map = {c.title: c.id for c in sheet.columns}
    else:
        # No existing sheet — create one from our template.
        # The template already has Dependencies Enabled and the correct columns,
        # so the new sheet inherits both without any extra configuration.
        new_sheet = smartsheet.models.Sheet({
            "name": sheet_name,
            "fromId": TEMPLATE_SHEET_ID,  # tells Smartsheet to copy the template
        })
        result = ss.Home.create_sheet_from_template(new_sheet)
        sheet_id = result.result.id
        # Fetch the new sheet to read its column IDs
        sheet = ss.Sheets.get_sheet(sheet_id)
        col_map = {c.title: c.id for c in sheet.columns}

    def make_row(act):
        """Convert a single activity dict into a Smartsheet Row object."""
        row = smartsheet.models.Row()
        row.to_bottom = True  # Append each row at the bottom of the sheet

        # Format duration: store as an integer if it's a whole number (e.g. 3 not 3.0)
        dur = act.get("duration", 0)
        fields = {
            "Task Name":    act.get("task_name", ""),
            "WBS":          act.get("wbs", ""),
            "Start":        act.get("start", ""),      # MM/DD/YYYY string
            "Finish":       act.get("finish", ""),     # MM/DD/YYYY string
            "Duration":     int(dur) if dur == int(dur) else dur,  # days
            "Predecessors": act.get("predecessors", ""),  # e.g. "3FS", "5SS+2d"
            "Assigned To":  act.get("assigned_to", ""),
        }
        # Build the cell list using column IDs from col_map
        for col_name, value in fields.items():
            if col_name in col_map:
                cell = smartsheet.models.Cell()
                cell.column_id = col_map[col_name]
                cell.value = value
                row.cells.append(cell)
        return row

    # Add rows in batches of 500 (Smartsheet API limit per call)
    batch_size = 500
    for i in range(0, len(activities), batch_size):
        batch = activities[i:i + batch_size]
        rows = [make_row(a) for a in batch]
        ss.Sheets.add_rows(sheet_id, rows)

    # Fetch the sheet one final time to get its permalink URL and return it
    sheet_info = ss.Sheets.get_sheet(sheet_id, row_numbers=None, column_ids=None)
    return sheet_info.permalink


if __name__ == "__main__":
    # Entry point for local development; Railway uses gunicorn instead
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
