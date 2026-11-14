"""SQLite 访问（薄封装）。

表：
- events  归档事件；主键 (source, line_no) 保证重复启动/重复 seed 不会重复导入
- runs    每次合并运行的元信息 + 完整结果 JSON（结果页、复核页、快照下载共用）
- published_versions  每次「已发布」的合并结果 + 水位线快照（按表、按版本不可变）
- published_current   每表当前生效的发布版本指针（结果与水位线一起切换）
- pending_events      迟到 / 同键待补事件队列（业务时间排序，重试不重复入队）
- backfill_batches    补传批次审计（幂等批次键、前后水位线、状态、失败原因）
- meta    seed 版本等键值
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator

from . import settings
from .archive import Event, load_events_file, parse_ts

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
  source         TEXT    NOT NULL,
  line_no        INTEGER NOT NULL,
  table_name     TEXT    NOT NULL,
  pk             TEXT    NOT NULL,
  version        INTEGER NOT NULL,
  ts             TEXT    NOT NULL,
  op             TEXT    NOT NULL,
  after_json     TEXT    NOT NULL,
  schema_version INTEGER NOT NULL,
  PRIMARY KEY (source, line_no)
);
CREATE INDEX IF NOT EXISTS idx_events_table ON events(table_name, line_no);

CREATE TABLE IF NOT EXISTS runs (
  run_id         TEXT PRIMARY KEY,
  created_at     TEXT NOT NULL,
  tables_json    TEXT NOT NULL,
  schema_version INTEGER NOT NULL,
  summary_json   TEXT NOT NULL,
  payload_json   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_runs_created ON runs(created_at DESC);

CREATE TABLE IF NOT EXISTS published_versions (
  version_id       TEXT PRIMARY KEY,
  table_name       TEXT NOT NULL,
  version_no       INTEGER NOT NULL,
  parent_version_no INTEGER,
  batch_key        TEXT NOT NULL,
  source           TEXT NOT NULL,
  created_at       TEXT NOT NULL,
  watermark        TEXT,
  rows_json        TEXT NOT NULL,
  tombstones_json  TEXT NOT NULL,
  conflicts_json   TEXT NOT NULL,
  changelog_json   TEXT NOT NULL,
  counters_json    TEXT NOT NULL,
  key_max_ts_json  TEXT NOT NULL,
  applied_ids_json TEXT NOT NULL,
  ignored_json     TEXT NOT NULL,
  watermark_json   TEXT NOT NULL,
  UNIQUE(table_name, version_no),
  UNIQUE(table_name, batch_key)
);
CREATE INDEX IF NOT EXISTS idx_published_table ON published_versions(table_name, version_no DESC);

CREATE TABLE IF NOT EXISTS published_current (
  table_name TEXT PRIMARY KEY,
  version_id TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pending_events (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  table_name     TEXT NOT NULL,
  source         TEXT NOT NULL,
  line_no        INTEGER NOT NULL,
  pk             TEXT NOT NULL,
  version        INTEGER NOT NULL,
  ts             TEXT NOT NULL,
  op             TEXT NOT NULL,
  after_json     TEXT NOT NULL,
  schema_version INTEGER NOT NULL,
  reason         TEXT NOT NULL,
  boundary       TEXT,
  detail         TEXT,
  tombstone_version INTEGER,
  status         TEXT NOT NULL DEFAULT 'pending',
  batch_key      TEXT,
  applied_version_id TEXT,
  created_at     TEXT NOT NULL,
  applied_at     TEXT,
  UNIQUE(table_name, source, line_no)
);
CREATE INDEX IF NOT EXISTS idx_pending_table ON pending_events(table_name, status, ts);

CREATE TABLE IF NOT EXISTS backfill_batches (
  batch_key        TEXT PRIMARY KEY,
  table_name       TEXT NOT NULL,
  status           TEXT NOT NULL,
  created_at       TEXT NOT NULL,
  applied_at       TEXT,
  version_id       TEXT,
  version_no       INTEGER,
  version_before   INTEGER,
  watermark_before TEXT,
  watermark_after  TEXT,
  cutoff           TEXT,
  pending_before   INTEGER NOT NULL DEFAULT 0,
  pending_after    INTEGER NOT NULL DEFAULT 0,
  affected_keys_json TEXT NOT NULL DEFAULT '[]',
  reasons_json     TEXT NOT NULL DEFAULT '[]',
  result_json      TEXT NOT NULL DEFAULT '{}',
  error            TEXT,
  attempts         INTEGER NOT NULL DEFAULT 1,
  replayed         INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_batches_table ON backfill_batches(table_name, created_at DESC);

CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
"""


def connect() -> sqlite3.Connection:
    settings.ensure_data_dir()
    conn = sqlite3.connect(settings.db_path())
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# --- meta ------------------------------------------------------------------


def set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute("INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)", (key, value))
    conn.commit()


def get_meta(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row["value"] if row else None


# --- events ----------------------------------------------------------------


def archive_events(conn: sqlite3.Connection, events: list[Event]) -> dict:
    """幂等归档：主键冲突一律忽略（重复启动/重复 seed 不产生脏数据）。"""
    inserted = 0
    for event in events:
        row = event.to_row()
        cursor = conn.execute(
            """INSERT OR IGNORE INTO events
               (source, line_no, table_name, pk, version, ts, op, after_json, schema_version)
               VALUES (:source, :line_no, :table, :pk, :version, :ts, :op, :after_json, :schema_version)""",
            row,
        )
        inserted += cursor.rowcount
    conn.commit()
    return {"attempted": len(events), "inserted": inserted, "skipped": len(events) - inserted}


def seed_sample(conn: sqlite3.Connection, events_dir: Path | None = None) -> dict:
    """把 sample_data/events/*.jsonl 逐表幂等导入；返回本次导入统计。"""
    events_dir = events_dir or settings.EVENTS_DIR
    total = {"attempted": 0, "inserted": 0, "skipped": 0, "tables": {}}
    for item in settings.TABLES:
        path = events_dir / item["file"]
        events = load_events_file(path, item["name"])
        stats = archive_events(conn, events)
        total["attempted"] += stats["attempted"]
        total["inserted"] += stats["inserted"]
        total["skipped"] += stats["skipped"]
        total["tables"][item["name"]] = stats
    set_meta(conn, "seed_version", settings.SEED_VERSION)
    set_meta(conn, "seeded_at", now_iso())
    total["seed_version"] = settings.SEED_VERSION
    return total


def _row_to_event(row: sqlite3.Row) -> Event:
    return Event(
        table=row["table_name"],
        line_no=int(row["line_no"]),
        pk=row["pk"],
        version=int(row["version"]),
        ts=parse_ts(row["ts"]),
        op=row["op"],
        after=json.loads(row["after_json"]),
        schema_version=int(row["schema_version"]),
        source=row["source"],
    )


def fetch_events(conn: sqlite3.Connection, table: str) -> list[Event]:
    """按到达顺序返回某表归档事件。

    归档时逐行插入，SQLite 隐式 rowid 即到达顺序：同一文件内就是文件行号；
    补传新增文件时，晚入档的文件排在原有事件之后（不会被误当作「先到」）。
    """
    rows = conn.execute(
        "SELECT * FROM events WHERE table_name=? ORDER BY rowid", (table,)
    ).fetchall()
    return [_row_to_event(row) for row in rows]


def event_counts(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute(
        "SELECT table_name, COUNT(*) AS n FROM events GROUP BY table_name"
    ).fetchall()
    return {row["table_name"]: int(row["n"]) for row in rows}


def total_events(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) AS n FROM events").fetchone()
    return int(row["n"])


# --- runs ------------------------------------------------------------------


def save_run(conn: sqlite3.Connection, run: dict) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO runs
           (run_id, created_at, tables_json, schema_version, summary_json, payload_json)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            run["run_id"],
            run["created_at"],
            json.dumps(run["tables"], ensure_ascii=False),
            int(run["schema_version"]),
            json.dumps(run["summary"], ensure_ascii=False),
            json.dumps(run["result"], ensure_ascii=False),
        ),
    )
    conn.commit()


def get_run(conn: sqlite3.Connection, run_id: str) -> dict | None:
    row = conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
    if row is None:
        return None
    return {
        "run_id": row["run_id"],
        "created_at": row["created_at"],
        "tables": json.loads(row["tables_json"]),
        "schema_version": int(row["schema_version"]),
        "summary": json.loads(row["summary_json"]),
        "result": json.loads(row["payload_json"]),
    }


def list_runs(conn: sqlite3.Connection, limit: int = 20) -> list[dict]:
    rows = conn.execute(
        "SELECT run_id, created_at, tables_json, schema_version, summary_json FROM runs "
        "ORDER BY created_at DESC, run_id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    out = []
    for row in rows:
        out.append(
            {
                "run_id": row["run_id"],
                "created_at": row["created_at"],
                "tables": json.loads(row["tables_json"]),
                "schema_version": int(row["schema_version"]),
                "summary": json.loads(row["summary_json"]),
            }
        )
    return out


def run_count(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) AS n FROM runs").fetchone()
    return int(row["n"])


def event_overview(conn: sqlite3.Connection) -> dict[str, dict]:
    """首页概览：按表统计 op / schema 版本 / 键数 / 时间区间（全部来自归档表）。"""
    out: dict[str, dict] = {}
    for item in settings.TABLES:
        table = item["name"]
        rows = conn.execute(
            "SELECT op, schema_version, pk, ts FROM events WHERE table_name=?", (table,)
        ).fetchall()
        ops: dict[str, int] = {}
        versions: dict[str, int] = {}
        stamps: list[str] = []
        keys: set[str] = set()
        for row in rows:
            ops[row["op"]] = ops.get(row["op"], 0) + 1
            versions[str(row["schema_version"])] = versions.get(str(row["schema_version"]), 0) + 1
            keys.add(row["pk"])
            stamps.append(row["ts"])
        out[table] = {
            "table": table,
            "label": item["label"],
            "desc": item["desc"],
            "file": item["file"],
            "events": len(rows),
            "ops": ops,
            "schema_versions": versions,
            "keys": len(keys),
            "first_ts": min(stamps) if stamps else None,
            "last_ts": max(stamps) if stamps else None,
        }
    return out


# --- 事务 ------------------------------------------------------------------


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """显式写事务：要么整体提交，要么整体回滚（发布切换与队列标记在同一事务里）。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()


# --- 发布版本与当前指针 ----------------------------------------------------


PUBLISHED_FIELDS = (
    "rows",
    "tombstones",
    "conflicts",
    "changelog",
    "counters",
    "key_max_ts",
    "applied_ids",
    "ignored",
    "watermark",
)


def _published_row(version: dict) -> dict:
    return {
        "version_id": version["version_id"],
        "table_name": version["table"],
        "version_no": int(version["version_no"]),
        "parent_version_no": version.get("parent_version_no"),
        "batch_key": version["batch_key"],
        "source": version["source"],
        "created_at": version["created_at"],
        "watermark": version.get("watermark"),
        "rows_json": json.dumps(version["rows"], ensure_ascii=False),
        "tombstones_json": json.dumps(version["tombstones"], ensure_ascii=False),
        "conflicts_json": json.dumps(version["conflicts"], ensure_ascii=False),
        "changelog_json": json.dumps(version["changelog"], ensure_ascii=False),
        "counters_json": json.dumps(version["counters"], ensure_ascii=False),
        "key_max_ts_json": json.dumps(version["key_max_ts"], ensure_ascii=False),
        "applied_ids_json": json.dumps(version["applied_ids"], ensure_ascii=False),
        "ignored_json": json.dumps(version["ignored"], ensure_ascii=False),
        "watermark_json": json.dumps(version["watermark_detail"], ensure_ascii=False),
    }


def save_published_version(conn: sqlite3.Connection, version: dict) -> dict:
    """写入一个不可变的发布版本（调用方负责事务与指针切换）。"""
    conn.execute(
        """INSERT INTO published_versions
           (version_id, table_name, version_no, parent_version_no, batch_key, source,
            created_at, watermark, rows_json, tombstones_json, conflicts_json,
            changelog_json, counters_json, key_max_ts_json, applied_ids_json,
            ignored_json, watermark_json)
           VALUES (:version_id, :table_name, :version_no, :parent_version_no, :batch_key,
                   :source, :created_at, :watermark, :rows_json, :tombstones_json,
                   :conflicts_json, :changelog_json, :counters_json, :key_max_ts_json,
                   :applied_ids_json, :ignored_json, :watermark_json)""",
        _published_row(version),
    )
    return version


def _decode_published(row: sqlite3.Row) -> dict:
    return {
        "version_id": row["version_id"],
        "table": row["table_name"],
        "version_no": int(row["version_no"]),
        "parent_version_no": row["parent_version_no"],
        "batch_key": row["batch_key"],
        "source": row["source"],
        "created_at": row["created_at"],
        "watermark": row["watermark"],
        "rows": json.loads(row["rows_json"]),
        "tombstones": json.loads(row["tombstones_json"]),
        "conflicts": json.loads(row["conflicts_json"]),
        "changelog": json.loads(row["changelog_json"]),
        "counters": json.loads(row["counters_json"]),
        "key_max_ts": json.loads(row["key_max_ts_json"]),
        "applied_ids": json.loads(row["applied_ids_json"]),
        "ignored": json.loads(row["ignored_json"]),
        "watermark_detail": json.loads(row["watermark_json"]),
    }


def set_current_version(conn: sqlite3.Connection, table: str, version_id: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO published_current(table_name, version_id, updated_at) VALUES(?, ?, ?)",
        (table, version_id, now_iso()),
    )


def get_current_version(conn: sqlite3.Connection, table: str) -> dict | None:
    row = conn.execute(
        """SELECT v.*, c.updated_at AS current_since
           FROM published_current c JOIN published_versions v ON v.version_id = c.version_id
           WHERE c.table_name=?""",
        (table,),
    ).fetchone()
    if row is None:
        return None
    version = _decode_published(row)
    version["current_since"] = row["current_since"]
    return version


def get_version_by_id(conn: sqlite3.Connection, version_id: str) -> dict | None:
    row = conn.execute(
        "SELECT * FROM published_versions WHERE version_id=?", (version_id,)
    ).fetchone()
    return _decode_published(row) if row else None


def version_for_batch(conn: sqlite3.Connection, table: str, batch_key: str) -> dict | None:
    row = conn.execute(
        "SELECT * FROM published_versions WHERE table_name=? AND batch_key=?",
        (table, batch_key),
    ).fetchone()
    return _decode_published(row) if row else None


def list_versions(conn: sqlite3.Connection, table: str | None = None, limit: int = 50) -> list[dict]:
    if table:
        rows = conn.execute(
            "SELECT * FROM published_versions WHERE table_name=? ORDER BY version_no DESC LIMIT ?",
            (table, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM published_versions ORDER BY created_at DESC, version_no DESC LIMIT ?",
            (limit,),
        ).fetchall()
    out = []
    for row in rows:
        version = _decode_published(row)
        out.append(
            {
                "version_id": version["version_id"],
                "table": version["table"],
                "version_no": version["version_no"],
                "parent_version_no": version["parent_version_no"],
                "batch_key": version["batch_key"],
                "source": version["source"],
                "created_at": version["created_at"],
                "watermark": version["watermark"],
                "counters": version["counters"],
                "key_max_ts": version["key_max_ts"],
                "applied_ids_count": len(version["applied_ids"]),
            }
        )
    return out


def all_current_versions(conn: sqlite3.Connection) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for item in settings.TABLES:
        version = get_current_version(conn, item["name"])
        if version is not None:
            out[item["name"]] = version
    return out


def current_version_numbers(conn: sqlite3.Connection) -> dict[str, int]:
    return {table: version["version_no"] for table, version in all_current_versions(conn).items()}


# --- 待补队列 --------------------------------------------------------------


def enqueue_pending(conn: sqlite3.Connection, items: list[dict]) -> dict:
    """幂等入队：唯一键 (table, source, line_no)；重复入队返回已存在记录，不新增。"""
    added, skipped = 0, 0
    for item in items:
        cursor = conn.execute(
            """INSERT OR IGNORE INTO pending_events
               (table_name, source, line_no, pk, version, ts, op, after_json,
                schema_version, reason, boundary, detail, tombstone_version,
                status, created_at)
               VALUES (:table, :source, :line_no, :pk, :version, :ts, :op, :after_json,
                       :schema_version, :reason, :boundary, :detail, :tombstone_version,
                       'pending', :created_at)""",
            item,
        )
        if cursor.rowcount:
            added += 1
        else:
            skipped += 1
    return {"added": added, "skipped": skipped}


def pending_events_for_table(conn: sqlite3.Connection, table: str,
                             statuses: Iterable[str] = ("pending",)) -> list[dict]:
    placeholders = ",".join("?" for _ in statuses)
    rows = conn.execute(
        f"""SELECT * FROM pending_events
            WHERE table_name=? AND status IN ({placeholders})
            ORDER BY ts, source, line_no""",
        (table, *statuses),
    ).fetchall()
    return [dict(row) for row in rows]


def pending_summary(conn: sqlite3.Connection) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for item in settings.TABLES:
        table = item["name"]
        rows = conn.execute(
            "SELECT status, reason, COUNT(*) AS n, MIN(ts) AS oldest, MAX(ts) AS newest "
            "FROM pending_events WHERE table_name=? GROUP BY status, reason",
            (table,),
        ).fetchall()
        by_status: dict[str, int] = {}
        reasons: dict[str, int] = {}
        oldest = newest = None
        for row in rows:
            by_status[row["status"]] = by_status.get(row["status"], 0) + int(row["n"])
            reasons[row["reason"]] = reasons.get(row["reason"], 0) + int(row["n"])
            if oldest is None or (row["oldest"] and row["oldest"] < oldest):
                oldest = row["oldest"]
            if newest is None or (row["newest"] and row["newest"] > newest):
                newest = row["newest"]
        out[table] = {
            "table": table,
            "pending": by_status.get("pending", 0),
            "deferred": by_status.get("deferred", 0),
            "applied": by_status.get("applied", 0),
            "by_status": by_status,
            "reasons": reasons,
            "oldest_ts": oldest,
            "newest_ts": newest,
            "pending_events": pending_events_for_table(conn, table),
        }
    return out


def mark_pending_applied(conn: sqlite3.Connection, ids: list[int], batch_key: str,
                         version_id: str) -> None:
    stamp = now_iso()
    for pending_id in ids:
        conn.execute(
            "UPDATE pending_events SET status='applied', batch_key=?, applied_version_id=?, applied_at=? "
            "WHERE id=? AND status IN ('pending', 'deferred')",
            (batch_key, version_id, stamp, pending_id),
        )


def mark_pending_batch(conn: sqlite3.Connection, ids: list[int], batch_key: str, status: str) -> None:
    for pending_id in ids:
        conn.execute(
            "UPDATE pending_events SET status=?, batch_key=? WHERE id=?",
            (status, batch_key, pending_id),
        )


def pending_event_by_id(conn: sqlite3.Connection, pending_id: int) -> dict | None:
    row = conn.execute("SELECT * FROM pending_events WHERE id=?", (pending_id,)).fetchone()
    return dict(row) if row else None


# --- 补传批次 --------------------------------------------------------------


def get_batch(conn: sqlite3.Connection, batch_key: str) -> dict | None:
    row = conn.execute("SELECT * FROM backfill_batches WHERE batch_key=?", (batch_key,)).fetchone()
    return dict(row) if row else None


def list_batches(conn: sqlite3.Connection, table: str | None = None, limit: int = 50) -> list[dict]:
    if table:
        rows = conn.execute(
            "SELECT * FROM backfill_batches WHERE table_name=? ORDER BY created_at DESC LIMIT ?",
            (table, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM backfill_batches ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(row) for row in rows]


def save_batch(conn: sqlite3.Connection, batch: dict) -> None:
    payload = {
        "batch_key": batch["batch_key"],
        "table_name": batch["table"],
        "status": batch["status"],
        "created_at": batch["created_at"],
        "applied_at": batch.get("applied_at"),
        "version_id": batch.get("version_id"),
        "version_no": batch.get("version_no"),
        "version_before": batch.get("version_before"),
        "watermark_before": batch.get("watermark_before"),
        "watermark_after": batch.get("watermark_after"),
        "cutoff": batch.get("cutoff"),
        "pending_before": int(batch.get("pending_before", 0)),
        "pending_after": int(batch.get("pending_after", 0)),
        "affected_keys_json": json.dumps(batch.get("affected_keys", []), ensure_ascii=False),
        "reasons_json": json.dumps(batch.get("reasons", []), ensure_ascii=False),
        "result_json": json.dumps(batch.get("result", {}), ensure_ascii=False),
        "error": batch.get("error"),
        "attempts": int(batch.get("attempts", 1)),
        "replayed": int(batch.get("replayed", 0)),
    }
    conn.execute(
        """INSERT INTO backfill_batches
           (batch_key, table_name, status, created_at, applied_at, version_id, version_no,
            version_before, watermark_before, watermark_after, cutoff,
            pending_before, pending_after,
            affected_keys_json, reasons_json, result_json, error, attempts, replayed)
           VALUES (:batch_key, :table_name, :status, :created_at, :applied_at, :version_id,
                   :version_no, :version_before, :watermark_before, :watermark_after,
                   :cutoff, :pending_before, :pending_after,
                   :affected_keys_json, :reasons_json, :result_json,
                   :error, :attempts, :replayed)
           ON CONFLICT(batch_key) DO UPDATE SET
             status=excluded.status, applied_at=excluded.applied_at,
             version_id=excluded.version_id, version_no=excluded.version_no,
             version_before=excluded.version_before,
             watermark_before=excluded.watermark_before,
             watermark_after=excluded.watermark_after,
             cutoff=excluded.cutoff,
             pending_before=excluded.pending_before,
             pending_after=excluded.pending_after,
             affected_keys_json=excluded.affected_keys_json,
             reasons_json=excluded.reasons_json, result_json=excluded.result_json,
             error=excluded.error, attempts=excluded.attempts, replayed=excluded.replayed""",
        payload,
    )


def batch_to_dict(row: dict) -> dict:
    out = dict(row)
    out["table"] = out.pop("table_name")
    out["affected_keys"] = json.loads(out.pop("affected_keys_json") or "[]")
    out["reasons"] = json.loads(out.pop("reasons_json") or "[]")
    out["result"] = json.loads(out.pop("result_json") or "{}")
    return out


def batch_fingerprint(table: str, pending_ids: list[int], schema_version: int = 2) -> str:
    """批次键：同表 + 同一组待补事件 + 同目标 schema → 同一批，重试不会新增版本。"""
    material = json.dumps(
        {"table": table, "pending": sorted(int(p) for p in pending_ids), "schema": int(schema_version)},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return "B" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:12].upper()


def pending_ids_for_table(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(
        "SELECT source, line_no FROM pending_events WHERE table_name=?", (table,)
    ).fetchall()
    return {f"{row['source']}:{row['line_no']}" for row in rows}


def all_pending_rows(conn: sqlite3.Connection, table: str | None = None) -> list[dict]:
    if table:
        rows = conn.execute(
            "SELECT * FROM pending_events WHERE table_name=? ORDER BY table_name, ts, source, line_no",
            (table,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM pending_events ORDER BY table_name, ts, source, line_no"
        ).fetchall()
    return [dict(row) for row in rows]


def pending_rows_by_ids(conn: sqlite3.Connection, ids: list[int]) -> list[dict]:
    if not ids:
        return []
    placeholders = ",".join("?" for _ in ids)
    rows = conn.execute(
        f"SELECT * FROM pending_events WHERE id IN ({placeholders}) ORDER BY ts, source, line_no",
        tuple(ids),
    ).fetchall()
    return [dict(row) for row in rows]


def current_versions_for(conn: sqlite3.Connection, tables: list[str]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for table in tables:
        version = get_current_version(conn, table)
        if version is not None:
            out[table] = version
    return out


def pending_counts(conn: sqlite3.Connection, tables: list[str]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for table in tables:
        rows = conn.execute(
            "SELECT status, COUNT(*) AS n FROM pending_events WHERE table_name=? GROUP BY status",
            (table,),
        ).fetchall()
        counts = {row["status"]: int(row["n"]) for row in rows}
        out[table] = {
            "pending": counts.get("pending", 0),
            "applied": counts.get("applied", 0),
            "deferred": counts.get("deferred", 0),
            "total": sum(counts.values()),
        }
    return out


def get_version(conn: sqlite3.Connection, table: str, version_no: int) -> dict | None:
    row = conn.execute(
        "SELECT * FROM published_versions WHERE table_name=? AND version_no=?",
        (table, version_no),
    ).fetchone()
    return _decode_published(row) if row else None
