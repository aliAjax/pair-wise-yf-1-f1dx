"""SQLite 持久层：表结构、连接与初始化。

仅使用 Python 标准库 sqlite3，不依赖任何第三方包。
所有写操作走 ``BEGIN IMMEDIATE`` 串行化，配合唯一约束保证幂等。
"""
import os
import sqlite3
import threading

DEFAULT_DB_PATH = os.environ.get("ICE_DOCK_DB", "ice_dock.db")

# 单行状态表的固定主键
ICE_MAKER_ROW_ID = 1
NETWORK_ROW_ID = 1

_schema = """
-- 时段与容量
CREATE TABLE IF NOT EXISTS slots (
    slot_id  TEXT PRIMARY KEY,          -- 时段标识
    slot_time TEXT NOT NULL,            -- 到港时段（展示用）
    capacity REAL NOT NULL,             -- 容量(kg)
    occupied REAL NOT NULL DEFAULT 0    -- 已占用(kg)
);

-- 预约
CREATE TABLE IF NOT EXISTS appointments (
    appointment_id  TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,   -- 幂等键：重发不重复占容量
    boat_owner      TEXT NOT NULL,
    slot_id         TEXT NOT NULL REFERENCES slots(slot_id),
    booked_amount   REAL NOT NULL,          -- 预约量
    actual_amount   REAL,                   -- 实拿量(过磅)
    status          TEXT NOT NULL,          -- queued/booked/processing/confirmed/cancelled
    voyage_id       TEXT,                   -- 航次：结欠结转用
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_appt_boat   ON appointments(boat_owner);
CREATE INDEX IF NOT EXISTS idx_appt_slot   ON appointments(slot_id);
CREATE INDEX IF NOT EXISTS idx_appt_status ON appointments(status);

-- 取冰确认：同一条确认只记一次（幂等）
CREATE TABLE IF NOT EXISTS confirmations (
    confirmation_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    appointment_id  TEXT NOT NULL UNIQUE REFERENCES appointments(appointment_id),
    actual_slot_id  TEXT REFERENCES slots(slot_id),   -- 实际到港时段(早晚到撞时段)
    actual_amount   REAL NOT NULL,
    shortfall       REAL NOT NULL,                    -- 少拿的差额
    created_at      TEXT NOT NULL
);

-- 结欠：少拿的差额结转下一航次
CREATE TABLE IF NOT EXISTS balances (
    boat_owner TEXT PRIMARY KEY,
    amount     REAL NOT NULL DEFAULT 0     -- 正=欠船主的冰，可结转下一航次
);

-- 离线重发队列：断网时照常记，恢复后重发不重复占容量
CREATE TABLE IF NOT EXISTS outbox (
    outbox_id      TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL,
    kind           TEXT NOT NULL,          -- appointment / confirmation
    payload        TEXT NOT NULL,           -- JSON
    status         TEXT NOT NULL DEFAULT 'pending',  -- pending/synced
    created_at     TEXT NOT NULL,
    synced_at      TEXT
);
CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox(status);

-- 制冰机状态（单行）
CREATE TABLE IF NOT EXISTS ice_maker (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    status     TEXT NOT NULL,              -- running/stopped
    updated_at TEXT NOT NULL
);

-- 网络状态（单行）
CREATE TABLE IF NOT EXISTS network (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    status     TEXT NOT NULL,              -- up/down
    updated_at TEXT NOT NULL
);
"""

_init_lock = threading.Lock()


def get_db(path=DEFAULT_DB_PATH):
    """打开一个数据库连接。调用方负责关闭。"""
    conn = sqlite3.connect(path, timeout=15, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=15000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(path=DEFAULT_DB_PATH):
    """建表并初始化单行状态。幂等，可重复调用。"""
    with _init_lock:
        conn = get_db(path)
        try:
            conn.executescript(_schema)
            now = _now()
            conn.execute(
                "INSERT OR IGNORE INTO ice_maker(id, status, updated_at) VALUES(?, 'running', ?)",
                (ICE_MAKER_ROW_ID, now),
            )
            conn.execute(
                "INSERT OR IGNORE INTO network(id, status, updated_at) VALUES(?, 'up', ?)",
                (NETWORK_ROW_ID, now),
            )
            conn.commit()
        finally:
            conn.close()
    return path


def _now():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()
