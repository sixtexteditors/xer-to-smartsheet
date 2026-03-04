"""
XER Parser - Parses Primavera P6 XER files into structured data.
Extracts: PROJECT, WBS, TASK, TASKPRED, TASKRSRC, RSRC tables.
"""

from collections import defaultdict
from datetime import datetime


def parse_xer(file_content: str) -> dict:
    """
    Parse a P6 XER file and return structured data ready for Smartsheet import.
    Returns a dict with keys: project_name, activities (list of dicts)
    """
    tables = _split_tables(file_content)

    project = _parse_project(tables.get("PROJECT", []))
    wbs_map = _parse_wbs(tables.get("WBS", []))
    rsrc_map = _parse_rsrc(tables.get("RSRC", []))
    tasks, task_id_map = _parse_tasks(tables.get("TASK", []), wbs_map)
    taskpred = _parse_taskpred(tables.get("TASKPRED", []))
    taskrsrc = _parse_taskrsrc(tables.get("TASKRSRC", []), rsrc_map)

    for task in tasks:
        tid = task["_task_id"]
        preds = taskpred.get(tid, [])
        pred_strings = []
        for p in preds:
            pred_row_num = task_id_map.get(p["pred_task_id"])
            if pred_row_num is not None:
                lag_days = _lag_to_days(p.get("lag_hr_cnt", "0"))
                rel_type = _normalize_rel_type(p.get("pred_type", "PR_FS"))
                if lag_days != 0:
                    sign = "+" if lag_days > 0 else ""
                    pred_strings.append(f"{pred_row_num}{rel_type}{sign}{lag_days}d")
                else:
                    pred_strings.append(f"{pred_row_num}{rel_type}")
        task["predecessors"] = ",".join(pred_strings)


    for task in tasks:
        tid = task["_task_id"]
        resources = taskrsrc.get(tid, [])
        task["assigned_to"] = ", ".join(resources) if resources else ""

    for task in tasks:
        task.pop("_task_id", None)

    return {
        "project_name": project.get("proj_short_name", "Imported Project"),
        "activities": tasks,
    }


def _split_tables(content: str) -> dict:
    tables = defaultdict(list)
    current_table = None
    headers = []
    for line in content.splitlines():
        line = line.rstrip("\r")
        if not line:
            continue
        if line.startswith("%T"):
            current_table = line[3:].strip()
            headers = []
        elif line.startswith("%F"):
            headers = line[3:].split("\t")
        elif line.startswith("%R") and current_table and headers:
            values = line[3:].split("\t")
            row = dict(zip(headers, values))
            tables[current_table].append(row)
        elif line.startswith("%E"):
            break
    return tables


def _parse_project(rows):
    return rows[0] if rows else {}


def _parse_wbs(rows):
    wbs_by_id = {r["wbs_id"]: r for r in rows}
    def get_path(wbs_id, visited=None):
        if visited is None:
            visited = set()
        if wbs_id in visited:
            return ""  # Break cycle
        visited.add(wbs_id)
        node = wbs_by_id.get(wbs_id)
        if not node:
            return ""
        parent_id = node.get("parent_wbs_id", "")
        short = node.get("wbs_short_name") or node.get("wbs_name", "")
        if parent_id and parent_id in wbs_by_id:
            parent_path = get_path(parent_id, visited)
            return f"{parent_path}.{short}" if parent_path else short
        return short
    return {wid: get_path(wid) for wid in wbs_by_id}


def _parse_rsrc(rows):
    return {r["rsrc_id"]: r.get("rsrc_name", r.get("rsrc_short_name", "")) for r in rows}


def _parse_tasks(rows, wbs_map):
    tasks = []
    task_id_map = {}
    for i, row in enumerate(rows, start=1):
        task_id = row.get("task_id", "")
        task_id_map[task_id] = i
        duration_hrs = float(row.get("target_drtn_hr_cnt", "0") or "0")
        duration_days = round(duration_hrs / 8, 1)
        start_raw = row.get("target_start_date") or row.get("act_start_date", "")
        finish_raw = row.get("target_end_date") or row.get("act_end_date", "")
        tasks.append({
            "_task_id": task_id,
            "task_name": row.get("task_name", ""),
            "wbs": wbs_map.get(row.get("wbs_id", ""), ""),
            "start": _format_date(start_raw),
            "finish": _format_date(finish_raw),
            "duration": duration_days,
            "predecessors": "",
            "assigned_to": "",
        })
    return tasks, task_id_map


def _parse_taskpred(rows):
    result = defaultdict(list)
    for row in rows:
        result[row.get("task_id", "")].append({
            "pred_task_id": row.get("pred_task_id", ""),
            "pred_type": row.get("pred_type", "PR_FS"),
            "lag_hr_cnt": row.get("lag_hr_cnt", "0"),
        })
    return result


def _parse_taskrsrc(rows, rsrc_map):
    result = defaultdict(list)
    for row in rows:
        task_id = row.get("task_id", "")
        rsrc_name = rsrc_map.get(row.get("rsrc_id", ""), "")
        if rsrc_name and rsrc_name not in result[task_id]:
            result[task_id].append(rsrc_name)
    return result


def _format_date(date_str):
    if not date_str:
        return ""
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return date_str


def _lag_to_days(lag_hr_str):
    try:
        return round(float(lag_hr_str) / 8)
    except (ValueError, TypeError):
        return 0


def _normalize_rel_type(pred_type):
    return {"PR_FS": "FS", "PR_SS": "SS", "PR_FF": "FF", "PR_SF": "SF"}.get(pred_type, "FS")
