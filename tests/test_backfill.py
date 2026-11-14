"""核心规则 6/7：迟到事件进待补队列、按业务时间补齐、同批原子切换与幂等重试。"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app import archive, backfill, merge, runner, settings, store, watermark
from app.archive import Event

MIN = lambda minute: datetime(2026, 11, 10, 0, 0, tzinfo=timezone.utc).replace(minute=0) + __import__("datetime").timedelta(minutes=minute)  # noqa: E731
STAMP = lambda minute: MIN(minute).strftime("%Y-%m-%dT%H:%M:%SZ")  # noqa: E731


def ev(line_no: int, pk: str, version: int, minute: int, op: str = "u",
       after: dict | None = None, table: str = "orders", source: str = "orders.jsonl") -> Event:
    return Event(
        table=table, line_no=line_no, pk=pk, version=version, ts=MIN(minute), op=op,
        after=after if after is not None else {"pk": pk, "amount": 100 + version},
        schema_version=1, source=source,
    )


@pytest.fixture()
def seeded(tmp_path, monkeypatch):
    monkeypatch.setenv("CDCWATER_DB", str(tmp_path / "backfill.sqlite3"))
    conn = store.connect()
    store.init_db(conn)
    store.seed_sample(conn)
    try:
        yield conn
    finally:
        conn.close()


def publish_all(conn):
    run = runner.execute(conn, settings.TABLE_NAMES, 2)
    return run


# --- 首次发布：迟到只进队列 -------------------------------------------------


def test_first_publish_excludes_late_and_queues_them(seeded):
    run = publish_all(seeded)
    summary = run["summary"]
    assert summary["late"] == 8
    assert summary["pending"] == 8
    assert summary["pending_tombstone"] == 2
    assert summary["ignored_after_tombstone"] == 0   # 迟到未被合并，自然没有墓碑后忽略
    assert summary["published_version"] == {"orders": 1, "customers": 1, "inventory": 1}
    # 已发布结果与「只合并按时事件」完全一致
    events = store.fetch_events(seeded, "orders")
    late_lines = {mark.line_no for mark in watermark.detect_late(events)}
    baseline = merge.merge_table("orders", [e for e in events if e.line_no not in late_lines])
    current = store.get_current_version(seeded, "orders")
    assert [row["pk"] for row in current["rows"]] == [row["pk"] for row in baseline.rows]
    assert current["watermark"] == watermark.watermark_for_table("orders", baseline.key_max_ts).watermark


def test_queue_is_sorted_by_business_time(seeded):
    publish_all(seeded)
    rows = backfill.pending_rows(seeded, "orders")
    stamps = [row["ts"] for row in rows]
    assert stamps == sorted(stamps)
    assert rows[0]["line_no"] == 58      # 业务时间最早（00:05）
    assert rows[0]["reason"] == "within_watermark"
    assert rows[0]["tombstone_version"] == 3
    assert rows[0]["status"] == "pending"


def test_late_event_does_not_rewrite_published_result_before_backfill(seeded):
    publish_all(seeded)
    before = store.get_current_version(seeded, "orders")
    row = next(item for item in before["rows"] if item["pk"] == "O9004")
    assert row["version"] == 2          # 迟到 v3 未参与发布

    # 再跑一次合并：迟到事件仍在队列，结果不变
    again = publish_all(seeded)
    after = store.get_current_version(seeded, "orders")
    assert after["version_no"] == before["version_no"]
    assert again["summary"]["pending"] == 8
    assert next(item for item in after["rows"] if item["pk"] == "O9004")["version"] == 2


# --- 补齐与原子切换 ---------------------------------------------------------


def test_backfill_switches_result_and_watermark_together(seeded):
    publish_all(seeded)
    before = store.get_current_version(seeded, "orders")
    detail = backfill.apply_batch(seeded, "orders", cutoff=None, schema_version=2)
    assert detail["status"] == "applied"
    assert detail["version_no"] == 2
    assert detail["watermark_before"] == before["watermark"]
    after = store.get_current_version(seeded, "orders")
    assert detail["watermark_after"] == after["watermark"]
    assert after["version_id"] == detail["version_id"]
    # 迟到高版本胜出，且结果与水位线来自同一个版本
    row = next(item for item in after["rows"] if item["pk"] == "O9004")
    assert row["version"] == 3
    assert after["watermark_detail"]["laggard_keys"]


def test_backfill_keeps_tombstones_and_conflict_events(seeded):
    publish_all(seeded)
    detail = backfill.apply_batch(seeded, "orders", cutoff=None)
    after = store.get_current_version(seeded, "orders")
    tombstones = {item["pk"]: item for item in after["tombstones"]}
    assert tombstones["O9002"]["tombstone_version"] == 3
    assert tombstones["O9002"]["tombstone_line_no"] == 57
    # 墓碑后迟到事件补入时被忽略，但事件仍在队列里（status=applied）且保留行号
    ignored = [row for row in backfill.pending_rows(seeded, "orders") if row["pk"] == "O9002"]
    assert len(ignored) == 1
    assert ignored[0]["status"] == "applied"
    assert ignored[0]["line_no"] == 58
    assert 58 in after["ignored"]
    # 冲突事件保留：先到内容不被覆盖，两条事件都在明细里
    conflict = next(item for item in after["conflicts"] if item["pk"] == "O9001")
    assert conflict["first"]["line_no"] == 31
    assert conflict["second"]["line_no"] == 42
    assert conflict["diff"] == ["amount"]


def test_cutoff_expands_to_key_boundary(seeded):
    publish_all(seeded)
    detail = backfill.apply_batch(seeded, "orders", cutoff=STAMP(15))
    selected = detail["result"]["pending_ids"]
    # 00:05 的 O9002 与 00:15 的 O9003 同批；O9003 键边界 00:30 之前没有其他待补事件
    assert len(selected) == 2
    assert detail["pending_after"] == 1
    after = store.get_current_version(seeded, "orders")
    assert next(item for item in after["rows"] if item["pk"] == "O9003")["version"] == 3
    assert next(item for item in after["rows"] if item["pk"] == "O9004")["version"] == 2


# --- 失败与重试 -------------------------------------------------------------


def test_failure_keeps_old_version_and_queue(seeded):
    publish_all(seeded)
    before = store.get_current_version(seeded, "orders")
    with pytest.raises(backfill.BackfillError):
        backfill.apply_batch(seeded, "orders", cutoff=None, simulate_failure=True)
    after = store.get_current_version(seeded, "orders")
    assert after["version_id"] == before["version_id"]
    assert after["watermark"] == before["watermark"]
    assert len(store.pending_events_for_table(seeded, "orders")) == 3
    failed = [b for b in backfill.batches(seeded, "orders") if b["status"] == "failed"]
    assert len(failed) == 1
    assert failed[0]["watermark_after"] is None
    assert failed[0]["watermark_before"] == before["watermark"]


def test_failed_batch_retry_reuses_same_batch_key(seeded):
    publish_all(seeded)
    with pytest.raises(backfill.BackfillError):
        backfill.apply_batch(seeded, "orders", cutoff=None, simulate_failure=True)
    failed = [b for b in backfill.batches(seeded, "orders") if b["status"] == "failed"][0]
    detail = backfill.apply_batch(seeded, "orders", batch_key=failed["batch_key"])
    assert detail["batch_key"] == failed["batch_key"]
    assert detail["status"] == "applied"
    assert detail["attempts"] == 2
    versions = [v["version_no"] for v in backfill.versions(seeded, "orders")]
    assert versions == [2, 1]          # 重试只产生一个版本


def test_successful_batch_retry_does_not_add_version(seeded):
    publish_all(seeded)
    first = backfill.apply_batch(seeded, "orders", cutoff=None)
    second = backfill.apply_batch(seeded, "orders", batch_key=first["batch_key"])
    assert second["reused"] is True
    assert second["version_no"] == first["version_no"]
    assert [v["version_no"] for v in backfill.versions(seeded, "orders")] == [2, 1]


def test_repeated_same_cutoff_request_is_idempotent(seeded):
    publish_all(seeded)
    first = backfill.apply_batch(seeded, "orders", cutoff=STAMP(15))
    second = backfill.apply_batch(seeded, "orders", cutoff=STAMP(15))
    assert second["reused"] is True
    assert second["version_no"] == first["version_no"]
    assert [v["version_no"] for v in backfill.versions(seeded, "orders")] == [2, 1]


def test_backfill_without_pending_is_rejected(seeded):
    publish_all(seeded)
    backfill.apply_batch(seeded, "customers", cutoff=None)
    with pytest.raises(backfill.BackfillError):
        backfill.apply_batch(seeded, "customers", cutoff=STAMP(10))


# --- 冲突与墓碑事件的补入 ---------------------------------------------------


def test_pending_conflict_event_is_preserved(seeded):
    """补入的迟到事件若与已发布事件同版本不同内容，冲突保留，且不覆盖先到内容。"""
    table = "orders"
    publish_all(seeded)
    published = store.get_current_version(seeded, table)
    original = next(item for item in published["rows"] if item["pk"] == "O1001")
    token = "arrival-extra.jsonl"
    # 同键、同版本、不同内容，业务时间落在该键边界以内 → 只会进待补队列
    clash = ev(1, "O1001", int(original["version"]), 14, "u",
               dict(original["after"], amount=999999), source=token)
    store.archive_events(seeded, [clash])
    runner.execute(seeded, [table], 2)
    assert store.get_current_version(seeded, table)["version_no"] == 1  # 未直接改写
    queue = [row for row in backfill.pending_rows(seeded, table) if row["pk"] == "O1001"]
    assert len(queue) == 1 and queue[0]["line_no"] == 1
    assert queue[0]["reason"] == "within_watermark"
    detail = backfill.apply_batch(seeded, table, cutoff=None)
    after = store.get_current_version(seeded, table)
    record = next(item for item in after["conflicts"] if item["pk"] == "O1001")
    assert record["first"]["line_no"] == original["line_no"]      # 先到的已发布内容保留
    assert record["second"]["line_no"] == 1
    assert record["diff"] == ["amount"]      # 只有金额不同，其余字段一致
    assert next(item for item in after["rows"] if item["pk"] == "O1001")["after"]["amount"] == original["after"]["amount"]
    assert detail["result"]["conflicts_preserved"]


def test_new_event_beyond_key_boundary_publishes_immediately(seeded):
    publish_all(seeded)
    version_before = store.get_current_version(seeded, "orders")["version_no"]
    token = "arrival-new.jsonl"
    fresh = ev(1, "O8888", 1, 90, "c", {"amount": 10}, source=token)
    store.archive_events(seeded, [fresh])
    sync = backfill.sync_table(seeded, "orders", store.fetch_events(seeded, "orders"))
    assert sync["published"] == 1
    assert sync["queued"] == 0
    current = store.get_current_version(seeded, "orders")
    assert current["version_no"] == version_before + 1
    assert any(item["pk"] == "O8888" for item in current["rows"])


# --- 页面口径 ---------------------------------------------------------------


def test_run_result_carries_queue_and_watermark_pair(seeded):
    run = publish_all(seeded)
    result = run["result"]
    assert result["mode"] == "published"
    assert set(result["pending"]) == set(settings.TABLE_NAMES)
    orders_pending = result["pending"]["orders"]
    assert orders_pending["pending"] == 3
    assert {row["reason"] for row in orders_pending["rows"]} <= {
        "within_watermark", "late_arrival"
    }
    assert all(row["status"] == "pending" for row in orders_pending["rows"])
    assert result["watermark_before"]["orders"] is None    # 基线没有补前水位线
    assert result["versions"]["orders"]["version_no"] == 1


def test_api_backfill_flow(client):
    run = client.post("/api/merge", json={"tables": ["orders"], "schema_version": 2}).json()
    assert run["pending"] == 3
    first = client.post("/api/backfill", json={"table": "orders", "schema_version": 2}).json()
    assert first["status"] == "applied"
    assert first["pending_before"] == 3 and first["pending_after"] == 0
    assert first["watermark_before"] == "2026-11-10T00:14:00Z"
    assert first["watermark_after"] == "2026-11-10T00:14:00Z"
    assert first["result"]["tombstones_preserved"][0]["pk"] == "O9002"
    again = client.post("/api/backfill",
                        json={"table": "orders", "batch_key": first["batch_key"]}).json()
    assert again["reused"] is True
    assert again["version_no"] == first["version_no"]

    state = client.get("/api/backfill/state").json()["tables"]["orders"]
    assert state["pending_count"] == 0
    assert [v["version_no"] for v in state["versions"]] == [2, 1]

    page = client.get("/backfill")
    assert page.status_code == 200
    assert "待补队列" in page.text
    assert "补前水位线" in page.text


def test_api_failure_preserves_old_version(client):
    client.post("/api/merge", json={"tables": ["orders"], "schema_version": 2})
    before = client.get("/api/backfill/state").json()["tables"]["orders"]["current"]
    response = client.post("/api/backfill", json={"table": "orders", "simulate_failure": True})
    assert response.status_code == 400
    assert "模拟失败" in response.json()["detail"]
    after = client.get("/api/backfill/state").json()["tables"]["orders"]["current"]
    assert after == before
    assert client.get("/api/backfill/state").json()["tables"]["orders"]["pending_count"] == 3


# --- 快照契约 ---------------------------------------------------------------


def test_snapshot_exposes_pending_and_before_after_watermarks(client):
    run_id = client.post("/api/merge",
                         json={"tables": ["orders"], "schema_version": 2}).json()["run_id"]
    snapshot = client.get(f"/api/runs/{run_id}/snapshot").json()
    assert snapshot["export"]["mode"] == "published"
    assert snapshot["pending"]["orders"]["pending"] == 3
    assert snapshot["watermark_before"]["orders"] is None
    assert snapshot["versions"]["orders"]["version_no"] == 1
    assert snapshot["tables"]["orders"]["pending"]["pending"] == 3
    # 8 条迟到事件没有出现在变更日志中（未参与已发布结果）
    assert len(snapshot["changelog"]) == 57      # 只含 orders 的按时事件
    assert len(snapshot["late_events"]) == 3     # 只含 orders 的待补事件


# --- 原因分类与水位线推进 ---------------------------------------------------


def test_late_arrival_reason_and_watermark_advance_on_backfill(tmp_path, monkeypatch):
    """到达顺序迟到但业务时间晚于键边界 → late_arrival；补齐后水位线一起前移。"""
    monkeypatch.setenv("CDCWATER_DB", str(tmp_path / "reason.sqlite3"))
    conn = store.connect()
    store.init_db(conn)
    events = [
        ev(1, "A", 1, 10, "c", {"amount": 1}, source="t.jsonl"),
        ev(2, "B", 1, 30, "c", {"amount": 2}, source="t.jsonl"),
        ev(3, "A", 2, 20, "u", {"amount": 3}, source="t.jsonl"),   # 到达迟到，业务时间 20 > A 边界 10
    ]
    store.archive_events(conn, events)
    baseline = backfill.publish_baseline(conn, "orders", store.fetch_events(conn, "orders"))
    assert baseline["watermark"] == STAMP(10)          # min(10, 30)
    queue = backfill.pending_rows(conn, "orders")
    assert [(row["line_no"], row["reason"]) for row in queue] == [(3, "late_arrival")]
    assert queue[0]["boundary"] == STAMP(10)

    detail = backfill.apply_batch(conn, "orders", cutoff=None)
    assert detail["watermark_before"] == STAMP(10)
    assert detail["watermark_after"] == STAMP(20)
    after = store.get_current_version(conn, "orders")
    assert next(item for item in after["rows"] if item["pk"] == "A")["version"] == 2
    conn.close()


def test_pending_reason_counts_reach_summary(seeded):
    run = publish_all(seeded)
    reasons = run["summary"]["pending_reasons"]
    assert reasons == {"within_watermark": 8}
    assert run["summary"]["pending_tombstone"] == 2
