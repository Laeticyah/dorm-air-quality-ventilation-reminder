from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from contextlib import closing
from pathlib import Path
from typing import Any

try:
    import paho.mqtt.client as mqtt
except ImportError:  # MQTT is optional for local API-only development.
    mqtt = None

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

ROOT = Path(__file__).resolve().parent
DB_PATH = Path(os.getenv("AIR_DB_PATH", ROOT / "air_monitor.db"))
MQTT_HOST = os.getenv("MQTT_HOST", "")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = "home/+/+/data"
MQTT_STATUS_TOPIC = "home/+/+/status"
ONLINE_TIMEOUT_SECONDS = 90
ALARM_DEDUP_SECONDS = 15 * 60

app = FastAPI(title="宿舍空气质量监测 API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
_db_lock = threading.Lock()
_mqtt_client: Any = None


class ReadingIn(BaseModel):
    device_id: str = Field(min_length=1, max_length=64)
    room: str = Field(min_length=1, max_length=64)
    ts: int | None = None
    temp: float | None = None
    humi: float | None = None
    aq_raw: int | None = None
    aq_index: float | None = None
    lux: float | None = None
    rssi: int | None = None
    flag: str = Field(default="OK", max_length=20)


class ThresholdUpdate(BaseModel):
    device_id: str
    metric: str
    warn_lo: float | None = None
    warn_hi: float | None = None
    alert_lo: float | None = None
    alert_hi: float | None = None


class AlarmAck(BaseModel):
    acked_by: str = Field(min_length=1, max_length=64)


class ConfigUpdate(BaseModel):
    device_id: str = Field(min_length=1, max_length=64)
    sample_interval: int = Field(default=5, ge=1, le=3600)
    push_interval: int = Field(default=30, ge=5, le=3600)


def connect_db() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_PATH, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA busy_timeout=5000")
    return connection


def init_db() -> None:
    with _db_lock, closing(connect_db()) as db:
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS device (
                device_id TEXT PRIMARY KEY,
                room TEXT NOT NULL,
                name TEXT,
                last_seen INTEGER,
                online INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS reading (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL,
                ts INTEGER NOT NULL,
                temp REAL,
                humi REAL,
                aq_raw INTEGER,
                aq_index REAL,
                lux REAL,
                rssi INTEGER,
                flag TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_reading_device_ts ON reading(device_id, ts);
            CREATE TABLE IF NOT EXISTS threshold (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL,
                metric TEXT NOT NULL,
                warn_lo REAL,
                warn_hi REAL,
                alert_lo REAL,
                alert_hi REAL,
                UNIQUE(device_id, metric)
            );
            CREATE TABLE IF NOT EXISTS alarm (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL,
                metric TEXT NOT NULL,
                level TEXT NOT NULL,
                value REAL,
                ts INTEGER NOT NULL,
                acked INTEGER NOT NULL DEFAULT 0,
                acked_by TEXT
            );
            CREATE TABLE IF NOT EXISTS config (
                device_id TEXT PRIMARY KEY,
                sample_interval INTEGER NOT NULL DEFAULT 5,
                push_interval INTEGER NOT NULL DEFAULT 30
            );
            """
        )
        db.commit()


def save_reading(payload: ReadingIn) -> dict[str, Any]:
    timestamp = payload.ts or int(time.time())
    with _db_lock, closing(connect_db()) as db:
        db.execute(
            """
            INSERT INTO device(device_id, room, last_seen, online)
            VALUES (?, ?, ?, 1)
            ON CONFLICT(device_id) DO UPDATE SET
                room=excluded.room, last_seen=excluded.last_seen, online=1
            """,
            (payload.device_id, payload.room, timestamp),
        )
        db.execute(
            """
            INSERT INTO reading(device_id, ts, temp, humi, aq_raw, aq_index, lux, rssi, flag)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload.device_id,
                timestamp,
                payload.temp,
                payload.humi,
                payload.aq_raw,
                payload.aq_index,
                payload.lux,
                payload.rssi,
                payload.flag,
            ),
        )
        values = {
            "temp": payload.temp,
            "humi": payload.humi,
            "aq_index": payload.aq_index,
        }
        defaults = {
            "temp": (26.0, 29.0, 30.0),
            "humi": (60.0, 70.0, 75.0),
            "aq_index": (1.0, 1.5, 2.5),
        }
        for metric, value in values.items():
            if value is None:
                continue
            row = db.execute(
                """
                SELECT warn_hi, alert_lo, alert_hi FROM threshold
                WHERE device_id=? AND metric=?
                """,
                (payload.device_id, metric),
            ).fetchone()
            warn_hi, alert_lo, alert_hi = (
                (row["warn_hi"], row["alert_lo"], row["alert_hi"])
                if row
                else defaults[metric]
            )
            level = None
            if alert_hi is not None and value > alert_hi:
                level = "L3"
            elif warn_hi is not None and value > warn_hi:
                level = "L2"
            elif alert_lo is not None and value < alert_lo:
                level = "L3"
            if level and not _recent_alarm_exists(
                db, payload.device_id, metric, level, timestamp
            ):
                db.execute(
                    """
                    INSERT INTO alarm(device_id, metric, level, value, ts)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (payload.device_id, metric, level, value, timestamp),
                )
        db.commit()
    return {"accepted": True, "device_id": payload.device_id, "ts": timestamp}


def _recent_alarm_exists(
    db: sqlite3.Connection,
    device_id: str,
    metric: str,
    level: str,
    timestamp: int,
) -> bool:
    row = db.execute(
        """
        SELECT 1 FROM alarm
        WHERE device_id=? AND metric=? AND level=? AND ts>=?
        LIMIT 1
        """,
        (device_id, metric, level, timestamp - ALARM_DEDUP_SECONDS),
    ).fetchone()
    return row is not None


def update_device_status(device_id: str, online: bool, timestamp: int | None = None) -> None:
    with _db_lock, closing(connect_db()) as db:
        db.execute(
            "UPDATE device SET online=?, last_seen=COALESCE(?, last_seen) WHERE device_id=?",
            (1 if online else 0, timestamp, device_id),
        )
        db.commit()


def mqtt_message(_client: Any, _userdata: Any, message: Any) -> None:
    try:
        payload = ReadingIn.model_validate(json.loads(message.payload.decode("utf-8")))
        save_reading(payload)
    except Exception as exc:
        print(f"[MQTT] ignored invalid payload: {exc}")


def mqtt_status_message(_client: Any, _userdata: Any, message: Any) -> None:
    parts = message.topic.split("/")
    if len(parts) != 4 or parts[0] != "home" or parts[3] != "status":
        return
    device_id = parts[2]
    status = message.payload.decode("utf-8", errors="replace").strip().lower()
    update_device_status(device_id, status == "online")


def start_mqtt() -> None:
    global _mqtt_client
    if not MQTT_HOST or mqtt is None:
        return
    _mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    _mqtt_client.on_message = mqtt_message
    _mqtt_client.message_callback_add(MQTT_STATUS_TOPIC, mqtt_status_message)
    _mqtt_client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
    _mqtt_client.subscribe([(MQTT_TOPIC, 1), (MQTT_STATUS_TOPIC, 1)])
    _mqtt_client.loop_start()


@app.on_event("startup")
def startup() -> None:
    init_db()
    start_mqtt()


@app.on_event("shutdown")
def shutdown() -> None:
    if _mqtt_client is not None:
        _mqtt_client.loop_stop()
        _mqtt_client.disconnect()


@app.get("/api/health")
def health() -> dict[str, Any]:
    return {"status": "ok", "mqtt": bool(_mqtt_client), "db": str(DB_PATH)}


@app.post("/api/readings")
def create_reading(payload: ReadingIn) -> dict[str, Any]:
    return save_reading(payload)


@app.get("/api/devices")
def devices() -> list[dict[str, Any]]:
    with closing(connect_db()) as db:
        stale_before = int(time.time()) - ONLINE_TIMEOUT_SECONDS
        rows = db.execute(
            """
            SELECT device_id, room, name, last_seen,
                   CASE WHEN last_seen IS NOT NULL AND last_seen>=?
                        THEN online ELSE 0 END AS online
            FROM device ORDER BY device_id
            """,
            (stale_before,),
        ).fetchall()
    return [dict(row) for row in rows]


@app.get("/api/latest")
def latest(device_id: str | None = None) -> list[dict[str, Any]]:
    with closing(connect_db()) as db:
        if device_id:
            rows = db.execute(
                """
                SELECT r.*, d.room FROM reading r JOIN device d USING(device_id)
                WHERE r.device_id=? ORDER BY r.ts DESC LIMIT 1
                """,
                (device_id,),
            ).fetchall()
        else:
            rows = db.execute(
                """
                SELECT r.*, d.room FROM reading r JOIN device d USING(device_id)
                WHERE r.id IN (SELECT MAX(id) FROM reading GROUP BY device_id)
                ORDER BY r.device_id
                """
            ).fetchall()
    return [dict(row) for row in rows]


@app.get("/api/history")
def history(
    device_id: str,
    metric: str = Query("aq_index", pattern="^(temp|humi|aq_raw|aq_index|lux)$"),
    from_ts: int | None = Query(None, alias="from"),
    to_ts: int | None = Query(None, alias="to"),
    bucket: str = Query("raw", pattern="^(raw|1m|1h)$"),
) -> list[dict[str, Any]]:
    allowed = {"temp", "humi", "aq_raw", "aq_index", "lux"}
    if metric not in allowed:
        raise HTTPException(400, "unsupported metric")
    start = from_ts if from_ts is not None else int(time.time()) - 24 * 3600
    end = to_ts if to_ts is not None else int(time.time())
    expression = metric
    if bucket == "1m":
        expression = f"CAST(ts / 60 AS INTEGER) * 60"
    elif bucket == "1h":
        expression = f"CAST(ts / 3600 AS INTEGER) * 3600"
    with closing(connect_db()) as db:
        rows = db.execute(
            f"""
            SELECT {expression} AS ts, AVG({metric}) AS value
            FROM reading
            WHERE device_id=? AND ts BETWEEN ? AND ? AND {metric} IS NOT NULL
            GROUP BY {expression}
            ORDER BY ts
            """,
            (device_id, start, end),
        ).fetchall()
    return [{"ts": row["ts"], "value": row["value"], "bucket": bucket} for row in rows]


@app.put("/api/threshold")
def update_threshold(payload: ThresholdUpdate) -> dict[str, Any]:
    if payload.metric not in {"temp", "humi", "aq_index", "aq_raw", "lux"}:
        raise HTTPException(400, "unsupported metric")
    with _db_lock, closing(connect_db()) as db:
        db.execute(
            """
            INSERT INTO threshold(device_id, metric, warn_lo, warn_hi, alert_lo, alert_hi)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(device_id, metric) DO UPDATE SET
                warn_lo=excluded.warn_lo, warn_hi=excluded.warn_hi,
                alert_lo=excluded.alert_lo, alert_hi=excluded.alert_hi
            """,
            (
                payload.device_id,
                payload.metric,
                payload.warn_lo,
                payload.warn_hi,
                payload.alert_lo,
                payload.alert_hi,
            ),
        )
        db.commit()
    return {"updated": True}


@app.get("/api/config/{device_id}")
def get_config(device_id: str) -> dict[str, Any]:
    with closing(connect_db()) as db:
        row = db.execute(
            "SELECT device_id, sample_interval, push_interval FROM config WHERE device_id=?",
            (device_id,),
        ).fetchone()
    if row is None:
        return {"device_id": device_id, "sample_interval": 5, "push_interval": 30}
    return dict(row)


@app.put("/api/config")
def update_config(payload: ConfigUpdate) -> dict[str, Any]:
    with _db_lock, closing(connect_db()) as db:
        db.execute(
            """
            INSERT INTO config(device_id, sample_interval, push_interval)
            VALUES (?, ?, ?)
            ON CONFLICT(device_id) DO UPDATE SET
                sample_interval=excluded.sample_interval,
                push_interval=excluded.push_interval
            """,
            (payload.device_id, payload.sample_interval, payload.push_interval),
        )
        db.commit()
    return {"updated": True, **payload.model_dump()}


@app.get("/api/alarms")
def alarms(device_id: str | None = None, limit: int = Query(50, ge=1, le=500)) -> list[dict[str, Any]]:
    with closing(connect_db()) as db:
        if device_id:
            rows = db.execute(
                "SELECT * FROM alarm WHERE device_id=? ORDER BY ts DESC LIMIT ?",
                (device_id, limit),
            ).fetchall()
        else:
            rows = db.execute("SELECT * FROM alarm ORDER BY ts DESC LIMIT ?", (limit,)).fetchall()
    return [dict(row) for row in rows]


@app.post("/api/alarms/{alarm_id}/ack")
def acknowledge_alarm(alarm_id: int, payload: AlarmAck) -> dict[str, Any]:
    with _db_lock, closing(connect_db()) as db:
        cursor = db.execute(
            "UPDATE alarm SET acked=1, acked_by=? WHERE id=?",
            (payload.acked_by, alarm_id),
        )
        db.commit()
    if cursor.rowcount == 0:
        raise HTTPException(404, "alarm not found")
    return {"acked": True, "id": alarm_id}


WEB_ROOT = ROOT.parent / "web"
if WEB_ROOT.exists():
    app.mount("/", StaticFiles(directory=WEB_ROOT, html=True), name="web")
