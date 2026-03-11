"""
XER Parser - Parses Primavera P6 XER files into structured data.
Extracts: PROJECT, WBS, TASK, TASKPRED, TASKRSRC, RSRC tables.
"""

import re
from collections import defaultdict
from datetime import datetime


def parse_xer(file_content: str) -> dict:
    """
    Parse a P6 XER file and return structured data ready for Smartsheet import.
    Returns a dict with keys:
      - project_name: str
      - wbs_tree: ordered list of WBS node dicts (depth-first), each with
                  {wbs_id, wbs_name, parent_wbs_id, depth}
      - activities_by_wbs: dict mapping wbs_id -> list of activity dicts
      - activities_flat: all activities in XER order (each has _task_id, _wbs_id)
      - predecessor_map: dict mapping task_id -> list of predecessor info dicts
      - task_id_map: dict mapping task_id -> 1-based XER order index (for debugging)
    """
    tables = _split_tables(file_content)

    project = _parse_project(tables.get("PROJECT", []))
    wbs_rows = tables.get("PROJWBS", tables.get("WBS", []))
    wbs_tree = _build_wbs_tree(wbs_rows)
    rsrc_map = _parse_rsrc(tables.get("RSRC", []))
    tasks, task_id_map = _parse_tasks(tables.get("TASK", []))
    taskpred = _parse_taskpred(tables.get("TASKPRED", []))
    taskrsrc = _parse_taskrsrc(tables.get("TASKRSRC", []), rsrc_map)

    for task in tasks:
        tid = task["_task_id"]
        resources = taskrsrc.get(tid, [])
        task["assigned_to"] = ", ".join(resources) if resources else ""

    # Build wbs_id -> ancestors list (root-to-node order) for facility/type lookup
    wbs_node_map = {n["wbs_id"]: n for n in wbs_tree}
    wbs_ancestors = _build_wbs_ancestors(wbs_node_map)

    _facility_re = re.compile(r"facility\s*#?\s*(\d+)", re.IGNORECASE)
    _type_keywords = [
        ("engineering",    "Engineering"),
        ("procurement",    "Procurement"),
        ("construction",   "Construction"),
        ("commissioning",  "Commissioning"),
    ]

    for task in tasks:
        try:
            ancestors = wbs_ancestors.get(task["_wbs_id"], [])
            names = [a["wbs_name"] for a in ancestors]

            # Facility: first name matching "Facility #N" or "Facility N"
            facility = ""
            for name in names:
                m = _facility_re.search(name)
                if m:
                    facility = f"Facility {m.group(1)}"
                    break
            task["facility"] = facility

            # Activity Type: first ancestry name containing a keyword
            activity_type = ""
            for name in names:
                for keyword, label in _type_keywords:
                    if keyword in name.lower():
                        activity_type = label
                        break
                if activity_type:
                    break
            task["activity_type"] = activity_type
        except Exception:
            task["facility"] = ""
            task["activity_type"] = ""

    activities_by_wbs = defaultdict(list)
    for task in tasks:
        activities_by_wbs[task["_wbs_id"]].append(task)

    return {
        "project_name": project.get("proj_short_name", "Imported Project"),
        "wbs_tree": wbs_tree,
        "activities_by_wbs": dict(activities_by_wbs),
        "activities_flat": tasks,
        "predecessor_map": dict(taskpred),
        "task_id_map": task_id_map,
    }


def _build_wbs_tree(rows):
    """
    Build an ordered list of WBS nodes in depth-first order.
    Each node dict: {wbs_id, wbs_name, parent_wbs_id, depth}
    Root nodes are those whose parent_wbs_id is absent or not in the table.
    """
    if not rows:
        return []

    wbs_by_id = {r["wbs_id"]: r for r in rows}
    children_map = defaultdict(list)
    roots = []

    for wbs_id, node in wbs_by_id.items():
        parent_id = node.get("parent_wbs_id", "")
        if parent_id and parent_id in wbs_by_id:
            children_map[parent_id].append(wbs_id)
        else:
            roots.append(wbs_id)

    result = []

    def recurse(wbs_id, depth):
        node = wbs_by_id[wbs_id]
        parent_id = node.get("parent_wbs_id", "")
        result.append({
            "wbs_id": wbs_id,
            "wbs_name": node.get("wbs_name", node.get("wbs_short_name", "")),
            "parent_wbs_id": parent_id if parent_id in wbs_by_id else "",
            "depth": depth,
        })
        sorted_children = sorted(
            children_map.get(wbs_id, []),
            key=lambda cid: int(wbs_by_id[cid].get("seq_num", 0) or 0)
        )
        for child_id in sorted_children:
            recurse(child_id, depth + 1)

    for root_id in roots:
        recurse(root_id, 0)

    return result


def _build_wbs_ancestors(wbs_node_map: dict) -> dict:
    """
    Build a mapping of wbs_id -> list of ancestor nodes (root-to-node inclusive).
    Each entry includes the node itself at the end of the list.
    """
    cache = {}

    def get_ancestors(wbs_id):
        if wbs_id in cache:
            return cache[wbs_id]
        node = wbs_node_map.get(wbs_id)
        if not node:
            cache[wbs_id] = []
            return []
        parent_id = node.get("parent_wbs_id", "")
        if parent_id and parent_id in wbs_node_map:
            ancestors = get_ancestors(parent_id) + [node]
        else:
            ancestors = [node]
        cache[wbs_id] = ancestors
        return ancestors

    return {wbs_id: get_ancestors(wbs_id) for wbs_id in wbs_node_map}


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


def _parse_rsrc(rows):
    return {r["rsrc_id"]: r.get("rsrc_name", r.get("rsrc_short_name", "")) for r in rows}


def _parse_tasks(rows):
    tasks = []
    task_id_map = {}
    for i, row in enumerate(rows, start=1):
        if row.get("task_type") == "TT_WBS":
            continue
        task_id = row.get("task_id", "")
        task_id_map[task_id] = i
        duration_hrs = float(row.get("target_drtn_hr_cnt", "0") or "0")
        duration_days = round(duration_hrs / 8, 1)
        start_raw = row.get("target_start_date") or row.get("act_start_date", "")
        finish_raw = row.get("target_end_date") or row.get("act_end_date", "")
        tasks.append({
            "_task_id": task_id,
            "_wbs_id": row.get("wbs_id", ""),
            "activity_id": row.get("task_code", ""),
            "task_name": row.get("task_name", ""),
            "start": _format_date(start_raw),
            "finish": _format_date(finish_raw),
            "duration": duration_days,
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
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
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
