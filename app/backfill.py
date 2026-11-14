"""模块 7 · 待补队列与发布切换：迟到事件不再直接改写已发布结果。

口径（对外承诺）：
1. 迟到事件按**业务时间**进入待补队列；入队只记录位置，不改写任何已发布行。
   同一键的「边界」= 该键已发布事件的最大业务时间（key_max_ts）；事件 ts <= 边界
   记为 `within_watermark`（水位线以内迟到），否则记为 `late_arrival`（到达顺序迟到）。
   墓碑后迟到另记 `tombstone_version`，补入时仍按墓碑规则忽略，但事件与墓碑都保留。
2. 已发布结果是不可变版本：rows / tombstones / conflicts / changelog / 水位线一并存版本表，
   每表一条 `published_current` 指针。补传成功时，新版本写入与指针切换在**同一事务**；
   任何一步失败整体回滚，指针仍指向旧版本，旧结果与旧水位线原样可用。
3. 同一键补齐边界前事件后才切换：批次选中的事件会向后扩展到「受影响键边界」之内的
   全部待补事件，避免同键被拆成两批、结果与水位线错位。
4. 批次键 `batch_key = hash(table + 补齐后的事件集合 + 目标 schema)`：
   - 成功批次重试 → 直接返回原版本（`reused=True`），不新增版本；
   - 失败批次重试 → 复用同一批次键，成功后批次状态改写为 applied；
   - 相同「表 + cutoff」在无新待补事件时重试 → 命中已应用批次，不新增版本。
5. 墓碑与冲突事件永不删除：墓碑随版本保存；冲突留在版本 conflicts；待补事件补入后被忽略/
   仍冲突时，队列记录 `status=applied` 并保留原行，变更日志可逐条回溯。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime

from . import merge, store, watermark
from .archive import Event, parse_ts
from .tombstone import payload_diff

BASELINE_BATCH_KEY = "BASE"
BASELINE_SOURCE = "baseline"
BACKFILL_SOURCE = "backfill"
ARRIVAL_SOURCE = "arrival"
ALL_CUTOFF = "9999-12-31T23:59:59Z"

REASON_LABELS = {
    "within_watermark": "水位线以内迟到",
    "late_arrival": "到达顺序迟到（边界外）",
}

STATUS_LABELS = {
    "pending": "待补",
    "applied": "已补入",
    "deferred": "暂缓",
}


class BackfillError(ValueError):
    """可预期的补传错误（路由转 4xx，且不会改动已发布版本）。"""


def _iso(ts: datetime | None) -> str | None:
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ") if ts else None


def _event_id(event: Event) -> str:
    return f"{event.source}:{event.line_no}"


def _line_id(source: str, line_no: int) -> str:
    return f"{source}:{line_no}"


def reason_label(reason: str) -> str:
    return REASON_LABELS.get(reason, reason)


def status_label(status: str) -> str:
    return STATUS_LABELS.get(status, status)


# --- 版本构造 --------------------------------------------------------------


def _build_version(
    *,
    table: str,
    result: merge.MergeResult,
    mark: watermark.TableWatermark,
    version_no: int,
    parent_version_no: int | None,
    batch_key: str,
    source: str,
    applied_ids: list[str],
    cutoff: str | None,
) -> dict:
    return {
        "version_id": f"{table[:2].upper()}V{version_no}-{batch_key[-6:]}",
        "table": table,
        "version_no": version_no,
        "parent_version_no": parent_version_no,
        "batch_key": batch_key,
        "source": source,
        "created_at": store.now_iso(),
        "watermark": mark.watermark,
        "cutoff": cutoff,
        "rows": result.rows,
        "tombstones": result.tombstones,
        "conflicts": result.conflicts,
        "changelog": result.changelog,
        "counters": result.counters,
        "key_max_ts": {pk: _iso(ts) for pk, ts in result.key_max_ts.items()},
        "applied_ids": applied_ids,
        "ignored": result.ignored_line_nos,
        "watermark_detail": mark.to_dict(),
    }


def _merge_applied(table: str, events: list[Event],
                   late_lines: set[int]) -> tuple[merge.MergeResult, watermark.TableWatermark]:
    result = merge.merge_table(table, events, late_lines)
    mark = watermark.watermark_for_table(table, result.key_max_ts)
    return result, mark


def _pending_item(event: Event, reason: str, boundary: datetime | None,
                  detail: str, tombstone_version: int | None) -> dict:
    return {
        "table": event.table,
        "source": event.source,
        "line_no": event.line_no,
        "pk": event.pk,
        "version": event.version,
        "ts": event.ts_iso,
        "op": event.op,
        "after_json": json.dumps(event.after, ensure_ascii=False),
        "schema_version": event.schema_version,
        "reason": reason,
        "boundary": _iso(boundary),
        "detail": detail,
        "tombstone_version": tombstone_version,
        "created_at": store.now_iso(),
    }


def _tombstone_version_for(result: merge.MergeResult, event: Event) -> int | None:
    """事件补入后是否会落在墓碑之后（仅用于页面标注，不影响入队条件）。"""
    tombstone = next((item for item in result.tombstones if item["pk"] == event.pk), None)
    if tombstone is None:
        return None
    value = tombstone.get("tombstone_version")
    return int(value) if value is not None else None


def _queue_event(result: merge.MergeResult, event: Event) -> dict:
    boundary = result.key_max_ts.get(event.pk)
    if boundary is not None and event.ts <= boundary:
        reason = "within_watermark"
        detail = (
            f"业务时间 {event.ts_iso} 不晚于该键边界 {_iso(boundary)}；"
            "补入前不参与已发布结果，补齐同键边界前事件后整体切换"
        )
    else:
        reason = "late_arrival"
        detail = (
            f"按到达顺序判迟到（当时已处理最大 ts 更高）；该键边界 "
            f"{_iso(boundary) or '—'} 早于本条，补入会推进该键边界"
        )
    tomb_version = _tombstone_version_for(result, event)
    if tomb_version is not None and event.version < tomb_version:
        detail += f"；墓碑 v{tomb_version} 更高，补入时按规则忽略，墓碑保留"
    return _pending_item(event, reason, boundary, detail, tomb_version)


# --- 基线发布 --------------------------------------------------------------


def publish_baseline(conn, table: str, events: list[Event], schema_version: int = 2) -> dict:
    """首次发布：只归并「按时到达」的事件；迟到事件全部进入待补队列。

    基线版本、队列与批次审计在同一事务写入；重复调用直接返回现有版本。
    """
    existing = store.get_current_version(conn, table)
    if existing is not None:
        return existing

    late_marks = watermark.detect_late(events)
    late_lines = {mark.line_no for mark in late_marks}
    on_time = [event for event in events if event.line_no not in late_lines]
    result, mark = _merge_applied(table, on_time, set())
    applied_ids = [_event_id(event) for event in on_time]
    queued = [_queue_event(result, event) for event in events if event.line_no in late_lines]

    version = _build_version(
        table=table,
        result=result,
        mark=mark,
        version_no=1,
        parent_version_no=None,
        batch_key=BASELINE_BATCH_KEY,
        source=BASELINE_SOURCE,
        applied_ids=applied_ids,
        cutoff=None,
    )
    with store.transaction(conn):
        current = store.get_current_version(conn, table)
        if current is not None:
            return current
        store.save_published_version(conn, version)
        store.set_current_version(conn, table, version["version_id"])
        store.enqueue_pending(conn, queued)
        if store.get_batch(conn, f"{BASELINE_BATCH_KEY}:{table}") is None:
            store.save_batch(
                conn,
                {
                    "batch_key": f"{BASELINE_BATCH_KEY}:{table}",
                    "table": table,
                    "status": "applied",
                    "created_at": store.now_iso(),
                    "applied_at": store.now_iso(),
                    "version_id": version["version_id"],
                    "version_no": 1,
                    "version_before": None,
                    "watermark_before": None,
                    "watermark_after": mark.watermark,
                    "pending_before": 0,
                    "pending_after": len(queued),
                    "cutoff": None,
                    "affected_keys": sorted({item["pk"] for item in queued}),
                    "reasons": sorted({item["reason"] for item in queued}),
                    "result": {
                        "kind": "baseline",
                        "on_time_events": len(on_time),
                        "queued_events": len(queued),
                        "watermark_before": None,
                        "watermark_after": mark.watermark,
                        "final_rows": len(result.rows),
                        "final_tombstones": len(result.tombstones),
                        "note": "基线首发只合并按时事件；迟到事件全部进入待补队列",
                    },
                    "attempts": 1,
                    "replayed": 0,
                },
            )
    return store.get_current_version(conn, table)


# --- 版本重算 --------------------------------------------------------------


def _excluded_pending_lines(conn, table: str) -> set[str]:
    rows = conn.execute(
        "SELECT source, line_no FROM pending_events WHERE table_name=? "
        "AND status IN ('pending', 'deferred')",
        (table,),
    ).fetchall()
    return {_line_id(row["source"], int(row["line_no"])) for row in rows}


def _queued_line_nos(conn, table: str) -> set[int]:
    rows = conn.execute(
        "SELECT line_no FROM pending_events WHERE table_name=?", (table,)
    ).fetchall()
    return {int(row["line_no"]) for row in rows}


def _recompute_version(conn, table: str, *, batch_key: str, source: str,
                       remaining_pending: set[str], late_lines: set[int],
                       version_no: int, parent: dict | None, cutoff: str | None) -> dict:
    rows = conn.execute(
        "SELECT * FROM events WHERE table_name=? ORDER BY rowid", (table,)
    ).fetchall()
    applied: list[Event] = []
    applied_ids: list[str] = []
    for row in rows:
        event_id = _line_id(row["source"], int(row["line_no"]))
        if event_id in remaining_pending:
            continue
        applied.append(store._row_to_event(row))
        applied_ids.append(event_id)
    result, mark = _merge_applied(table, applied, late_lines)
    return _build_version(
        table=table,
        result=result,
        mark=mark,
        version_no=version_no,
        parent_version_no=parent["version_no"] if parent else None,
        batch_key=batch_key,
        source=source,
        applied_ids=applied_ids,
        cutoff=cutoff,
    )


# --- 前后对比 --------------------------------------------------------------


def _state_for_key(version: dict, pk: str) -> dict | None:
    for row in version["rows"]:
        if row["pk"] == pk:
            return {"deleted": False, **row}
    for row in version["tombstones"]:
        if row["pk"] == pk:
            return {"deleted": True, **row}
    return None


def _diff_state(before: dict | None, after: dict | None) -> dict:
    if before is None and after is None:
        return {"changed": False, "change_type": "absent"}
    if before is None:
        return {"changed": True, "change_type": "created", "changed_fields": []}
    if after is None:
        return {"changed": True, "change_type": "removed", "changed_fields": []}
    changed = []
    if bool(before.get("deleted")) != bool(after.get("deleted")):
        changed.append("deleted")
    if int(before.get("version", 0)) != int(after.get("version", 0)):
        changed.append("version")
    for field in payload_diff(before.get("after") or {}, after.get("after") or {}):
        changed.append(field)
    return {
        "changed": bool(changed),
        "change_type": "updated" if changed else "unchanged",
        "changed_fields": sorted(set(changed)),
    }


def _batch_result(conn, table: str, before: dict, after: dict, affected_keys: list[str],
                  selected_pending: list[dict], cutoff: str | None,
                  pending_before: int, pending_after: int,
                  schema_version: int = 2) -> dict:
    changes = []
    for pk in affected_keys:
        prev = _state_for_key(before, pk)
        now = _state_for_key(after, pk)
        diff = _diff_state(prev, now)
        changes.append(
            {
                "pk": pk,
                "before": prev,
                "after": now,
                "changed": diff["changed"],
                "change_type": diff.get("change_type"),
                "changed_fields": diff.get("changed_fields", []),
            }
        )
    keys = set(affected_keys)
    return {
        "cutoff": cutoff,
        "schema_version": int(schema_version),
        "pending_ids": [int(item["id"]) for item in selected_pending],
        "pending_count": len(selected_pending),
        "changes": changes,
        "changed_keys": sum(1 for item in changes if item["changed"]),
        "unchanged_keys": sum(1 for item in changes if not item["changed"]),
        "conflicts_preserved": [item for item in after["conflicts"] if item["pk"] in keys],
        "tombstones_preserved": [item for item in after["tombstones"] if item["pk"] in keys],
        "rows_before": len(before["rows"]),
        "rows_after": len(after["rows"]),
        "tombstones_before": len(before["tombstones"]),
        "tombstones_after": len(after["tombstones"]),
        "watermark_before": before["watermark"],
        "watermark_after": after["watermark"],
        "open_keys_before": before["watermark_detail"].get("open_keys"),
        "open_keys_after": after["watermark_detail"].get("open_keys"),
        "pending_before": pending_before,
        "pending_after": pending_after,
        "version_before": before["version_no"],
        "version_after": after["version_no"],
        "final_rows": after["rows"],
        "final_tombstones": after["tombstones"],
    }


# --- 待补选择与批次键 ------------------------------------------------------


def _normalize_cutoff(cutoff: str | None) -> str | None:
    if cutoff is None or str(cutoff).strip() == "":
        return None
    text = str(cutoff).strip()
    if text in ("all", "全部"):
        return ALL_CUTOFF
    return _iso(parse_ts(text))


def _key_boundaries(version: dict) -> dict[str, str]:
    return {pk: ts for pk, ts in version.get("key_max_ts", {}).items() if ts}


def select_pending(conn, table: str, cutoff: str | None, current: dict) -> list[dict]:
    """按业务时间选择待补事件，并为受影响键补齐边界前的全部事件。

    - 先取 `ts <= cutoff` 的待补事件；
    - 再对每个受影响键扩展：同一键 ts <= max(键边界, 已选最大 ts) 的事件必须同批，
      避免同键被批次边界拆开，导致结果与水位线不同步切换；
    - 若同键还留有更晚的待补事件（补入后会继续推进该键边界），整段一起纳入本批，
      保证「同一键补齐边界前事件后」一次切换到位。
    """
    rows = store.pending_events_for_table(conn, table, statuses=("pending", "deferred"))
    if cutoff is not None:
        rows = [row for row in rows if row["ts"] <= cutoff]
    if not rows:
        return []

    boundaries = _key_boundaries(current)
    all_open = store.pending_events_for_table(conn, table, statuses=("pending", "deferred"))
    by_key: dict[str, list[dict]] = {}
    for row in all_open:
        by_key.setdefault(row["pk"], []).append(row)
    selected: dict[int, dict] = {int(row["id"]): row for row in rows}
    changed = True
    while changed:
        changed = False
        for pk in {row["pk"] for row in selected.values()}:
            mine = sorted(by_key.get(pk, []), key=lambda row: row["ts"])
            chosen = [row for row in mine if int(row["id"]) in selected]
            if not chosen:
                continue
            boundary = boundaries.get(pk)
            limit = max([row["ts"] for row in chosen] + ([boundary] if boundary else []))
            for row in mine:
                if row["ts"] <= limit and int(row["id"]) not in selected:
                    selected[int(row["id"])] = row
                    changed = True
            # 仍有更晚的待补事件：同键整段一起补，避免下一批再推进同一键边界
            if any(row["ts"] > limit for row in mine):
                for row in mine:
                    if int(row["id"]) not in selected:
                        selected[int(row["id"])] = row
                        changed = True
    return sorted(selected.values(), key=lambda row: (row["ts"], row["source"], row["line_no"]))


def _fingerprint(table: str, pending_ids: list[int], schema_version: int) -> str:
    material = json.dumps(
        {"table": table, "pending": sorted(int(p) for p in pending_ids),
         "schema": int(schema_version)},
        ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    )
    return "B" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:12].upper()


def _apply_arrival(conn, table: str, current: dict, ahead: list[Event],
                   schema_version: int = 2) -> dict | None:
    """水位线之外的按时增量：单独一个版本，与补传同样原子切换。"""
    batch_key = "ARR-" + hashlib.sha256(
        ",".join(sorted(_event_id(e) for e in ahead)).encode("utf-8")
    ).hexdigest()[:12].upper()
    if store.version_for_batch(conn, table, batch_key) is not None:
        return store.batch_to_dict(store.get_batch(conn, batch_key) or {})
    affected = sorted({event.pk for event in ahead})
    pending_before = len(store.pending_events_for_table(conn, table, statuses=("pending", "deferred")))
    try:
        with store.transaction(conn):
            latest = store.get_current_version(conn, table)
            if latest is None:
                raise BackfillError(f"表 {table} 还没有已发布版本")
            version = _recompute_version(
                conn, table, batch_key=batch_key, source=ARRIVAL_SOURCE,
                remaining_pending=_excluded_pending_lines(conn, table),
                late_lines=_queued_line_nos(conn, table),
                version_no=int(latest["version_no"]) + 1, parent=latest, cutoff=None,
            )
            store.save_published_version(conn, version)
            store.set_current_version(conn, table, version["version_id"])
            result = _batch_result(
                conn, table, latest, version, affected, [], None,
                pending_before, pending_before, schema_version,
            )
            store.save_batch(
                conn,
                {
                    "batch_key": batch_key, "table": table, "status": "applied",
                    "created_at": store.now_iso(), "applied_at": store.now_iso(),
                    "version_id": version["version_id"], "version_no": version["version_no"],
                    "version_before": latest["version_no"],
                    "watermark_before": latest["watermark"],
                    "watermark_after": version["watermark"],
                    "pending_before": pending_before, "pending_after": pending_before,
                    "cutoff": None, "affected_keys": affected,
                    "reasons": ["arrival"], "result": result, "attempts": 1, "replayed": 0,
                },
            )
    except BackfillError:
        raise
    except Exception as exc:
        raise BackfillError(f"增量发布失败，已保留旧版本：{exc}") from exc
    return store.batch_to_dict(store.get_batch(conn, batch_key))


def _replay_batch(conn, batch_key: str) -> dict:
    with store.transaction(conn):
        replay = store.get_batch(conn, batch_key) or {}
        replay["attempts"] = int(replay.get("attempts", 1)) + 1
        replay["replayed"] = int(replay.get("replayed", 0)) + 1
        if replay.get("status") != "applied":
            raise BackfillError(f"批次 {batch_key} 尚未成功，不能复用")
        store.save_batch(conn, store.batch_to_dict(replay))
    detail = store.batch_to_dict(store.get_batch(conn, batch_key))
    detail["reused"] = True
    return detail


def _failure_batch(batch_key: str, table: str, current: dict, *, created_at: str,
                   attempts: int, cutoff: str | None, ids: list[int],
                   affected_keys: list[str], reasons: list[str],
                   pending_before: int, error: str) -> dict:
    return {
        "batch_key": batch_key, "table": table, "status": "failed",
        "created_at": created_at, "applied_at": None,
        "version_id": None, "version_no": None,
        "version_before": current["version_no"],
        "watermark_before": current["watermark"],
        "watermark_after": None,
        "pending_before": pending_before, "pending_after": pending_before,
        "cutoff": cutoff, "affected_keys": affected_keys, "reasons": reasons,
        "result": {
            "pending_ids": ids, "cutoff": cutoff, "stage": "publish",
            "note": "失败已整体回滚：已发布版本与水位线保持不变，待补队列原样保留",
            "watermark_before": current["watermark"], "watermark_after": None,
        },
        "error": error, "attempts": attempts, "replayed": 0,
    }


# --- 补传：单批原子切换 ----------------------------------------------------


def apply_batch(conn, table: str, cutoff: str | None = None,
                schema_version: int = 2, simulate_failure: bool = False,
                batch_key: str | None = None) -> dict:
    """把待补队列中业务时间 <= cutoff 的事件补齐，并原子切换结果与水位线。

    返回批次详情（含前后水位线、受影响键前后最终结果、保留的墓碑与冲突）。
    """
    current = store.get_current_version(conn, table)
    if current is None:
        raise BackfillError(f"表 {table} 还没有已发布版本，请先执行一次合并")

    normalized = _normalize_cutoff(cutoff)

    if batch_key:
        known = store.get_batch(conn, batch_key)
        if known is None:
            raise BackfillError(f"批次 {batch_key} 不存在")
        if known["table_name"] != table:
            raise BackfillError(f"批次 {batch_key} 属于表 {known['table_name']}，不是 {table}")
        if store.version_for_batch(conn, table, batch_key) is not None:
            return _replay_batch(conn, batch_key)
        # 失败批次重试：复用同一批次键与同一组待补事件，不新增批次
        known_detail = store.batch_to_dict(known)
        stored_ids = [int(item) for item in known_detail["result"].get("pending_ids", [])]
        rows = store.pending_rows_by_ids(conn, stored_ids)
        if len(rows) != len(stored_ids):
            raise BackfillError(
                f"批次 {batch_key} 的部分待补事件已不在队列中，无法按原批重试"
            )
        return _publish(conn, table, current, rows, _normalize_cutoff(known.get("cutoff")),
                        schema_version, simulate_failure, batch_key,
                        int(known["attempts"]) + 1, known["created_at"])

    selected = select_pending(conn, table, normalized, current)
    if not selected:
        previous = _find_applied_batch_for(conn, table, normalized, schema_version)
        if previous is not None:
            return _replay_batch(conn, previous["batch_key"])
        raise BackfillError(
            f"表 {table} 在补传边界 {normalized or '（全部）'} 前没有待补事件"
        )

    ids = [int(row["id"]) for row in selected]
    fingerprint = _fingerprint(table, ids, schema_version)
    if store.version_for_batch(conn, table, fingerprint) is not None:
        return _replay_batch(conn, fingerprint)

    existing_batch = store.get_batch(conn, fingerprint)
    attempts = (int(existing_batch["attempts"]) + 1) if existing_batch else 1
    batch_started = (existing_batch or {}).get("created_at") or store.now_iso()
    return _publish(conn, table, current, selected, normalized, schema_version,
                    simulate_failure, fingerprint, attempts, batch_started)


def _publish(conn, table: str, current: dict, selected: list[dict], normalized: str | None,
             schema_version: int, simulate_failure: bool, fingerprint: str,
             attempts: int, batch_started: str) -> dict:
    """补齐给定待补事件并原子切换；失败回滚到 current 指向的旧版本。"""
    ids = [int(row["id"]) for row in selected]
    affected_keys = sorted({row["pk"] for row in selected})
    reasons = sorted({row["reason"] for row in selected})
    pending_before = len(store.pending_events_for_table(conn, table, statuses=("pending", "deferred")))
    pending_after = max(0, pending_before - len(selected))
    selected_set = {_line_id(row["source"], int(row["line_no"])) for row in selected}
    remaining = _excluded_pending_lines(conn, table) - selected_set
    late_lines = _queued_line_nos(conn, table)

    try:
        with store.transaction(conn):
            latest = store.get_current_version(conn, table)
            if latest is None or latest["version_no"] != current["version_no"]:
                raise BackfillError(f"表 {table} 发布指针已变化，请重试")
            version = _recompute_version(
                conn, table, batch_key=fingerprint, source=BACKFILL_SOURCE,
                remaining_pending=remaining, late_lines=late_lines,
                version_no=int(latest["version_no"]) + 1, parent=latest, cutoff=normalized,
            )
            if simulate_failure:
                raise BackfillError("模拟失败：补传在发布切换前被中断")
            store.save_published_version(conn, version)
            store.set_current_version(conn, table, version["version_id"])
            store.mark_pending_applied(conn, ids, fingerprint, version["version_id"])
            result = _batch_result(
                conn, table, latest, version, affected_keys, selected, normalized,
                pending_before, pending_after, schema_version,
            )
            store.save_batch(
                conn,
                {
                    "batch_key": fingerprint, "table": table, "status": "applied",
                    "created_at": batch_started, "applied_at": store.now_iso(),
                    "version_id": version["version_id"],
                    "version_no": version["version_no"],
                    "version_before": latest["version_no"],
                    "watermark_before": latest["watermark"],
                    "watermark_after": version["watermark"],
                    "pending_before": pending_before, "pending_after": pending_after,
                    "cutoff": normalized, "affected_keys": affected_keys,
                    "reasons": reasons, "result": result,
                    "attempts": attempts, "replayed": 0,
                },
            )
    except BackfillError as exc:
        with store.transaction(conn):
            store.save_batch(
                conn,
                _failure_batch(
                    fingerprint, table, current, created_at=batch_started,
                    attempts=attempts, cutoff=normalized, ids=ids,
                    affected_keys=affected_keys, reasons=reasons,
                    pending_before=pending_before, error=str(exc),
                ),
            )
        raise
    except Exception as exc:
        with store.transaction(conn):
            store.save_batch(
                conn,
                _failure_batch(
                    fingerprint, table, current, created_at=batch_started,
                    attempts=attempts, cutoff=normalized, ids=ids,
                    affected_keys=affected_keys, reasons=reasons,
                    pending_before=pending_before,
                    error=str(exc) or exc.__class__.__name__,
                ),
            )
        raise BackfillError(f"补传失败，已保留旧版本：{exc}") from exc

    detail = store.batch_to_dict(store.get_batch(conn, fingerprint))
    detail["reused"] = False
    return detail


def _find_applied_batch_for(conn, table: str, cutoff: str | None,
                            schema_version: int) -> dict | None:
    """无待补事件时，查找同一「表 + cutoff」的已应用补传批次，用于幂等重试。"""
    rows = store.list_batches(conn, table, limit=200)
    for row in rows:
        if row["status"] != "applied" or row["batch_key"].startswith(BASELINE_BATCH_KEY):
            continue
        if row["batch_key"].startswith("ARR-"):
            continue
        if row["cutoff"] != cutoff:
            continue
        stored = json.loads(row["result_json"] or "{}")
        if int(stored.get("schema_version", schema_version)) != int(schema_version):
            continue
        return row
    return None


# --- 增量同步 --------------------------------------------------------------


def _state_as_result(version: dict) -> merge.MergeResult:
    """把已发布版本包成 MergeResult，仅用于复用待补入队的原因判定。"""
    result = merge.MergeResult(table=version["table"])
    result.rows = version["rows"]
    result.tombstones = version["tombstones"]
    result.conflicts = version["conflicts"]
    result.changelog = version["changelog"]
    result.counters = version["counters"]
    result.ignored_line_nos = version["ignored"]
    result.key_max_ts = {
        pk: parse_ts(ts) for pk, ts in version.get("key_max_ts", {}).items()
    }
    return result


def sync_table(conn, table: str, events: list[Event], schema_version: int = 2) -> dict:
    """确保该表有发布版本；把归档里尚未处理的新事件分流：

    - 业务时间 <= 该键边界 → 只入待补队列（不改写已发布结果）
    - 业务时间 > 该键边界 → 作为新版本原子发布
    """
    current = store.get_current_version(conn, table)
    if current is None:
        version = publish_baseline(conn, table, events, schema_version)
        return {
            "table": table,
            "created_baseline": True,
            "version_no": version["version_no"],
            "queued": len(store.pending_events_for_table(conn, table)),
            "published": 0,
        }

    known = set(current["applied_ids"])
    for row in store.all_pending_rows(conn, table):
        known.add(_line_id(row["source"], int(row["line_no"])))
    new_events = [event for event in events if _event_id(event) not in known]
    if not new_events:
        return {
            "table": table,
            "created_baseline": False,
            "version_no": current["version_no"],
            "queued": 0,
            "published": 0,
        }

    state = _state_as_result(current)
    queued, ahead = [], []
    for event in new_events:
        boundary = state.key_max_ts.get(event.pk)
        if boundary is not None and event.ts <= boundary:
            queued.append(_queue_event(state, event))
        else:
            ahead.append(event)

    if queued:
        with store.transaction(conn):
            store.enqueue_pending(conn, queued)

    if ahead:
        _apply_arrival(conn, table, current, ahead, schema_version)

    latest = store.get_current_version(conn, table)
    return {
        "table": table,
        "created_baseline": False,
        "version_no": latest["version_no"],
        "queued": len(queued),
        "published": len(ahead),
        "new_events": len(new_events),
    }


# --- 队列展示 --------------------------------------------------------------


def pending_rows(conn, table: str | None = None) -> list[dict]:
    rows = store.all_pending_rows(conn, table)
    out = []
    for row in rows:
        item = dict(row)
        item["reason_label"] = reason_label(item["reason"])
        item["status_label"] = status_label(item["status"])
        item["after"] = json.loads(item.get("after_json") or "{}")
        out.append(item)
    return out


def versions(conn, table: str | None = None, limit: int = 50) -> list[dict]:
    return store.list_versions(conn, table, limit)


def versions_with_batches(conn, table: str) -> list[dict]:
    """版本历史 + 生成它的批次；供页面并排展示前后水位线。"""
    out = []
    for version in store.list_versions(conn, table):
        batch = store.get_batch(conn, version["batch_key"])
        if batch is not None and batch["table_name"] == table:
            item = store.batch_to_dict(batch)
        else:
            item = None
        out.append({"version": version, "batch": item})
    return out


def batches(conn, table: str | None = None, limit: int = 50) -> list[dict]:
    rows = store.list_batches(conn, table, limit)
    out = []
    for row in rows:
        item = store.batch_to_dict(row)
        item["reason_labels"] = [reason_label(reason) for reason in item["reasons"]]
        item["status_label"] = {"applied": "已切换", "failed": "失败（保留旧版）"}.get(
            item["status"], item["status"]
        )
        out.append(item)
    return out


def state(conn: sqlite3.Connection, tables: list[str]) -> dict[str, dict]:
    """每表发布态：当前版本、水位线、待补计数与最老待补业务时间。"""
    out: dict[str, dict] = {}
    for table in tables:
        current = store.get_current_version(conn, table)
        pending = pending_rows(conn, table)
        open_rows = [item for item in pending if item["status"] != "applied"]
        batch_rows = batches(conn, table, limit=20)
        latest = next(
            (item for item in batch_rows
             if item["status"] == "applied" and item["result"].get("changes")),
            None,
        )
        out[table] = {
            "table": table,
            "current": current,
            "pending": pending,
            "open": open_rows,
            "pending_count": len(open_rows),
            "applied_count": sum(1 for item in pending if item["status"] == "applied"),
            "oldest_pending_ts": open_rows[0]["ts"] if open_rows else None,
            "newest_pending_ts": open_rows[-1]["ts"] if open_rows else None,
            "batches": batch_rows,
            "latest_batch": latest,
            "versions": versions_with_batches(conn, table),
        }
    return out
