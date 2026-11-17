"""SQLite 持久化：基准备份 / 核对点 / 恢复任务的状态全部落盘。

API 重启后据此续跑或按原点重试，不会丢状态、不会把半成品当成功。
"""
import sqlite3
import threading
from datetime import datetime, timezone

from . import config

_LOCK = threading.RLock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS backups (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    path        TEXT NOT NULL,
    status      TEXT NOT NULL,          -- running | done | failed
    start_lsn   TEXT,                   -- 备份起始 WAL 位点（恢复可用性的下界）
    timeline    INTEGER,
    size_bytes  INTEGER,
    error       TEXT,
    created_at  TEXT NOT NULL,
    finished_at TEXT
);
CREATE TABLE IF NOT EXISTS checkpoints (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE,
    restore_lsn TEXT NOT NULL,          -- pg_create_restore_point 返回的 LSN
    wal_file    TEXT NOT NULL,          -- 该 LSN 所在的 WAL 段文件名
    status      TEXT NOT NULL,          -- waiting_wal | ready | expired
    created_at  TEXT NOT NULL,
    ready_at    TEXT
);
CREATE TABLE IF NOT EXISTS jobs (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    checkpoint_name  TEXT NOT NULL,
    status           TEXT NOT NULL,     -- queued | waiting_wal | preparing | recovering
                                        -- | verifying | ready | failed
                                        -- | superseded | stopped | cancelled
    backup_id        INTEGER,           -- 本次恢复实际取用的备份代次（backups.id）
    detail           TEXT,
    error            TEXT,
    port             INTEGER,
    data_dir         TEXT,
    log_tail         TEXT,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL,
    ready_at         TEXT
);
-- 备份完整性核验记录：一次核验只绑定一个备份代次（backups.id），
-- 结果绝不跨代次复用；running 在 API 重启/中断后由启动流程改为 interrupted。
CREATE TABLE IF NOT EXISTS verifications (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    backup_id       INTEGER NOT NULL,   -- 备份代次（backups.id）
    status          TEXT NOT NULL,      -- running | passed | failed | interrupted
    started_at      TEXT NOT NULL,
    finished_at     TEXT,               -- 核验完成时间（verified_at）
    pid             INTEGER,            -- pg_verifybackup 进程号（中断清理用）
    manifest_sha256 TEXT,               -- 核验开始时的 backup_manifest 指纹
    error           TEXT,
    corruption      TEXT,               -- JSON 数组：损坏/校验失败明细
    output          TEXT                -- pg_verifybackup 输出（截断）
);
CREATE INDEX IF NOT EXISTS idx_verifications_backup
    ON verifications (backup_id, id DESC);
-- 服务级键值：integrity_gate_backup = 恢复解禁所需的最低备份代次。
-- 一旦某代核验失败/中断即写入；只有 >= 该代次的新核验通过才会删除。
CREATE TABLE IF NOT EXISTS service_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

GATE_KEY = "integrity_gate_backup"

# 进行中（未终结）的任务状态：一次只允许一个
ACTIVE_JOB_STATES = ("queued", "waiting_wal", "preparing", "recovering", "verifying")
# 允许发起重试的终结状态
RETRYABLE_JOB_STATES = ("failed", "stopped", "cancelled", "superseded")


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(config.STATE_DB, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=30000")
    return con


def init_db() -> None:
    with _LOCK:
        con = _connect()
        try:
            con.executescript(SCHEMA)
            _migrate(con)
            con.commit()
        finally:
            con.close()


def _migrate(con: sqlite3.Connection) -> None:
    """旧状态库升级：补列（幂等）。"""
    cols = {r["name"] for r in con.execute("PRAGMA table_info(jobs)")}
    if "backup_id" not in cols:
        con.execute("ALTER TABLE jobs ADD COLUMN backup_id INTEGER")


def execute(sql: str, params: tuple = ()) -> int:
    with _LOCK:
        con = _connect()
        try:
            cur = con.execute(sql, params)
            con.commit()
            return cur.lastrowid
        finally:
            con.close()


def all_rows(sql: str, params: tuple = ()) -> list[dict]:
    with _LOCK:
        con = _connect()
        try:
            return [dict(r) for r in con.execute(sql, params).fetchall()]
        finally:
            con.close()


def one_row(sql: str, params: tuple = ()) -> dict | None:
    rows = all_rows(sql, params)
    return rows[0] if rows else None


def update_job(job_id: int, **fields) -> None:
    if not fields:
        return
    fields["updated_at"] = now()
    cols = ", ".join(f"{k}=?" for k in fields)
    execute(f"UPDATE jobs SET {cols} WHERE id=?", (*fields.values(), job_id))


def get_meta(key: str) -> str | None:
    row = one_row("SELECT value FROM service_meta WHERE key=?", (key,))
    return row["value"] if row else None


def set_meta(key: str, value: str) -> None:
    execute(
        "INSERT INTO service_meta (key, value) VALUES (?,?)"
        " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )


def delete_meta(key: str) -> None:
    execute("DELETE FROM service_meta WHERE key=?", (key,))
