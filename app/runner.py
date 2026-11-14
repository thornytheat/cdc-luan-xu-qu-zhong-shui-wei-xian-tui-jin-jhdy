"""编排：把 6 个模块串成一次运行（纯函数部分可单测，不依赖数据库）。"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path

from . import archive, merge, schema_gate, settings, snapshot, store, watermark

EVENT_COUNTERS = (
    "events",
    "processed",
    "applied_insert",
    "applied_update",
    "applied_delete",
    "resurrected",
    "deduped_silent",
    "conflicts",
    "superseded",
    "ignored_after_tombstone",
    "late",
    "keys",
    "alive_rows",
    "tombstones",
)


class RunError(ValueError):
    """可预期的运行错误（路由转成 4xx）。"""


def new_run_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return f"R{stamp}-{uuid.uuid4().hex[:4].upper()}"


def validate_tables(tables: list[str]) -> list[str]:
    known = set(settings.TABLE_NAMES)
    unknown = [name for name in tables if name not in known]
    if unknown:
        raise RunError(
            f"未知的表：{', '.join(unknown)}；可选 {', '.join(settings.TABLE_NAMES)}"
        )
    ordered = [name for name in settings.TABLE_NAMES if name in set(tables)]
    if not ordered:
        raise RunError("至少选择一张表再执行合并")
    return ordered


def analyze(
    tables: list[str],
    events_by_table: dict[str, list[archive.Event]],
    base_doc: dict,
    target_doc: dict,
) -> dict:
    """一次完整的合并 + 水位线 + 门禁分析（纯函数，测试直接用）。"""
    late_by_table = {table: watermark.detect_late(events_by_table[table]) for table in tables}
    merged = merge.merge_tables({t: events_by_table[t] for t in tables}, late_by_table)

    table_marks: list[watermark.TableWatermark] = []
    payloads: dict[str, dict] = {}
    conflicts: list[dict] = []
    late_events: list[dict] = []
    changelog: list[dict] = []
    trajectory: dict[str, list[dict]] = {}
    counters = {key: 0 for key in EVENT_COUNTERS}

    for table in tables:
        result = merged[table]
        mark = watermark.watermark_for_table(table, result.key_max_ts)
        table_marks.append(mark)
        conflicts.extend(result.conflicts)
        late_events.extend(item.to_dict() for item in late_by_table[table])
        changelog.extend(result.changelog)
        trajectory[table] = watermark.trajectory(events_by_table[table], step=10)
        for key in EVENT_COUNTERS:
            counters[key] += int(result.counters.get(key, 0))
        payloads[table] = {
            "table": table,
            "label": settings.TABLE_LABELS.get(table, table),
            "rows": result.rows,
            "tombstones": result.tombstones,
            "row_count": len(result.rows),
            "tombstone_count": len(result.tombstones),
            "key_count": result.key_count,
            "counters": result.counters,
            "watermark": mark.to_dict(),
            "ignored_line_nos": result.ignored_line_nos,
        }

    gate_result = schema_gate.gate(base_doc, target_doc, tables)
    global_wm = watermark.combine(table_marks)
    result_payload = {
        "tables": payloads,
        "watermark": {
            **global_wm,
            "per_table": {mark.table: mark.to_dict() for mark in table_marks},
        },
        "counters": counters,
        "gate": gate_result.to_dict(),
        "conflicts": conflicts,
        "late_events": late_events,
        "changelog": changelog,
        "trajectory": trajectory,
    }
    result_payload["summary"] = build_summary(result_payload)
    return result_payload


def build_summary(result: dict) -> dict:
    counters = result["counters"]
    gate = result["gate"]
    return {
        "events": counters["events"],
        "conflicts": len(result["conflicts"]),
        "late": len(result["late_events"]),
        "ignored_after_tombstone": counters["ignored_after_tombstone"],
        "deduped_silent": counters["deduped_silent"],
        "superseded": counters["superseded"],
        "alive_rows": counters["alive_rows"],
        "tombstones": counters["tombstones"],
        "keys": counters["keys"],
        "watermark": result["watermark"]["watermark"],
        "open_keys": result["watermark"]["open_keys"],
        "gate_verdict": gate["verdict"],
        "gate_counts": gate["counts"],
    }


def load_schemas(schema_version: int, schemas_dir: Path | None = None) -> tuple[dict, dict]:
    """基线固定为 v1，目标为所选版本（选 v1 即「无变更」）。"""
    schemas_dir = schemas_dir or settings.SCHEMAS_DIR
    base = schema_gate.load_schema(schemas_dir / "v1.json")
    if int(schema_version) == 1:
        return base, base
    target = schema_gate.load_schema(schemas_dir / f"v{schema_version}.json")
    return base, target


def execute(
    conn,
    tables: list[str],
    schema_version: int = 2,
    events_by_table: dict[str, list[archive.Event]] | None = None,
    use_published: bool = True,
) -> dict:
    """读归档事件 → 分析 → 落库；返回可直接渲染的运行记录。

    `use_published=True`（工作台默认）：迟到事件按业务时间进待补队列，
    结果只来自「已发布版本」，页面展示待补原因与前后水位线；
    `use_published=False` 保留旧的「全量直算」口径，仅用于对照与单测。
    """
    if schema_version not in (1, 2):
        raise RunError(f"schema 版本只支持 1 或 2，收到 {schema_version}")
    ordered = validate_tables(tables)

    if events_by_table is None:
        events_by_table = {table: store.fetch_events(conn, table) for table in ordered}
        missing = [table for table in ordered if not events_by_table[table]]
        if missing:
            raise RunError(f"表 {', '.join(missing)} 还没有归档事件，请先加载样例数据")

    base_doc, target_doc = load_schemas(schema_version)
    if use_published:
        result = analyze_published(conn, ordered, events_by_table, base_doc, target_doc,
                                   schema_version)
    else:
        result = analyze(ordered, events_by_table, base_doc, target_doc)
    run = {
        "run_id": new_run_id(),
        "created_at": store.now_iso(),
        "tables": ordered,
        "schema_version": int(schema_version),
        "summary": result["summary"],
        "result": result,
    }
    store.save_run(conn, run)
    return run


def analyze_published(
    conn,
    tables: list[str],
    events_by_table: dict[str, list[archive.Event]],
    base_doc: dict,
    target_doc: dict,
    schema_version: int = 2,
) -> dict:
    """已发布口径：迟到事件不参与结果，只进待补队列；水位线与结果同步来自发布版本。"""
    from . import backfill

    sync = {}
    for table in tables:
        sync[table] = backfill.sync_table(conn, table, events_by_table[table], schema_version)

    current = store.current_versions_for(conn, tables)
    missing = [table for table in tables if table not in current]
    if missing:
        raise RunError(f"表 {', '.join(missing)} 还没有可用的发布版本")

    table_marks: list[watermark.TableWatermark] = []
    payloads: dict[str, dict] = {}
    conflicts: list[dict] = []
    changelog: list[dict] = []
    trajectory: dict[str, list[dict]] = {}
    counters = {key: 0 for key in EVENT_COUNTERS}
    versions: dict[str, dict] = {}
    queues: dict[str, dict] = {}

    table_marks: list[watermark.TableWatermark] = []
    payloads: dict[str, dict] = {}
    conflicts: list[dict] = []
    changelog: list[dict] = []
    trajectory: dict[str, list[dict]] = {}
    counters = {key: 0 for key in EVENT_COUNTERS}
    versions: dict[str, dict] = {}
    queues: dict[str, dict] = {}

    for table in tables:
        version = current[table]
        before_watermark = None
        if version["parent_version_no"] is not None:
            parent = store.get_version(conn, table, int(version["parent_version_no"]))
            before_watermark = parent["watermark"] if parent else None
        bounds = version["watermark_detail"]
        mark = watermark.TableWatermark(
            table=table,
            watermark=version["watermark"],
            open_keys=int(bounds.get("open_keys", 0)),
            closed_keys=int(bounds.get("closed_keys", 0)),
            laggard_keys=bounds.get("laggard_keys", []),
            key_max_ts=version["key_max_ts"],
        )
        table_marks.append(mark)
        conflicts.extend(version["conflicts"])
        changelog.extend(version["changelog"])
        trajectory[table] = watermark.trajectory(
            [event for event in events_by_table[table]
             if _event_id(event) in set(version["applied_ids"])],
            step=10,
        )
        for key in EVENT_COUNTERS:
            counters[key] += int(version["counters"].get(key, 0))
        pending = backfill.pending_rows(conn, table)
        queues[table] = {
            "pending": sum(1 for item in pending if item["status"] == "pending"),
            "applied": sum(1 for item in pending if item["status"] == "applied"),
            "deferred": sum(1 for item in pending if item["status"] == "deferred"),
            "reasons": sorted({item["reason"] for item in pending if item["status"] != "applied"}),
            "rows": pending,
            "watermark_before": before_watermark,
        }
        versions[table] = version
        payloads[table] = {
            "table": table,
            "label": settings.TABLE_LABELS.get(table, table),
            "rows": version["rows"],
            "tombstones": version["tombstones"],
            "row_count": len(version["rows"]),
            "tombstone_count": len(version["tombstones"]),
            "key_count": len(version["rows"]) + len(version["tombstones"]),
            "counters": version["counters"],
            "watermark": mark.to_dict(),
            "ignored_line_nos": version["ignored"],
            "version_id": version["version_id"],
            "version_no": version["version_no"],
            "published_at": version["created_at"],
            "applied_ids": version["applied_ids"],
            "pending": queues[table],
            "sync": sync.get(table, {}),
        }
        counters["events"] = counters["events"] + len(events_by_table[table])
        counters["late"] = counters["late"] + queues[table]["pending"]

    for key in EVENT_COUNTERS:
        if key in ("events", "late"):
            continue
        counters[key] = sum(int(payload["counters"].get(key, 0)) for payload in payloads.values())
    counters["processed"] = sum(int(p["counters"].get("processed", 0)) for p in payloads.values())
    counters["keys"] = sum(int(p["key_count"]) for p in payloads.values())
    counters["alive_rows"] = sum(int(p["row_count"]) for p in payloads.values())
    counters["tombstones"] = sum(int(p["tombstone_count"]) for p in payloads.values())

    gate_result = schema_gate.gate(base_doc, target_doc, tables)
    global_wm = watermark.combine(table_marks)
    result_payload = {
        "mode": "published",
        "tables": payloads,
        "versions": {table: _version_summary(version) for table, version in versions.items()},
        "pending": {
            table: {
                "pending": queue["pending"],
                "applied": queue["applied"],
                "deferred": queue["deferred"],
                "reasons": queue["reasons"],
                "rows": [
                    {k: v for k, v in item.items() if k != "after_json"} | {"after": item["after"]}
                    for item in queue["rows"]
                ],
            }
            for table, queue in queues.items()
        },
        "watermark_before": {
            table: queue["watermark_before"] for table, queue in queues.items()
        },
        "watermark": {
            **global_wm,
            "per_table": {mark.table: mark.to_dict() for mark in table_marks},
        },
        "counters": counters,
        "gate": gate_result.to_dict(),
        "conflicts": conflicts,
        "late_events": _pending_late_events(queues),
        "changelog": changelog,
        "trajectory": trajectory,
    }
    result_payload["summary"] = build_summary(result_payload)
    result_payload["summary"]["pending"] = sum(q["pending"] for q in queues.values())
    reason_counts: dict[str, int] = {}
    tombstone_pending = 0
    for queue in queues.values():
        for item in queue["rows"]:
            if item["status"] == "applied":
                continue
            reason_counts[item["reason"]] = reason_counts.get(item["reason"], 0) + 1
            if item["tombstone_version"] is not None:
                tombstone_pending += 1
    result_payload["summary"]["pending_reasons"] = reason_counts
    result_payload["summary"]["pending_tombstone"] = tombstone_pending
    result_payload["summary"]["published_version"] = {
        table: version["version_no"] for table, version in versions.items()
    }
    return result_payload


def _event_id(event: archive.Event) -> str:
    return f"{event.source}:{event.line_no}"


def _version_summary(version: dict) -> dict:
    return {
        "version_id": version["version_id"],
        "version_no": version["version_no"],
        "parent_version_no": version["parent_version_no"],
        "batch_key": version["batch_key"],
        "source": version["source"],
        "created_at": version["created_at"],
        "watermark": version["watermark"],
        "row_count": len(version["rows"]),
        "tombstone_count": len(version["tombstones"]),
    }


def _pending_late_events(queues: dict[str, dict]) -> list[dict]:
    out = []
    for table, queue in queues.items():
        for item in queue["rows"]:
            if item["status"] == "applied":
                continue
            out.append(
                {
                    "table": table,
                    "line_no": item["line_no"],
                    "pk": item["pk"],
                    "version": item["version"],
                    "ts": item["ts"],
                    "reason": item["reason"],
                    "boundary": item["boundary"],
                    "detail": item["detail"],
                    "pending_id": item["id"],
                    "status": item["status"],
                }
            )
    return out




def snapshot_for_run(run: dict) -> dict:
    return snapshot.build_snapshot(run)
