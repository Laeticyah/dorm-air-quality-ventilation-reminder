from __future__ import annotations

import json
import csv
import io
import logging
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
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

ROOT = Path(__file__).resolve().parent
DB_PATH = Path(os.getenv("AIR_DB_PATH", ROOT / "air_monitor.db"))
MQTT_HOST = os.getenv("MQTT_HOST", "")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")
MQTT_TLS = os.getenv("MQTT_TLS", "0").lower() in {"1", "true", "yes", "on"}
MQTT_TOPIC = "home/+/+/data"
MQTT_STATUS_TOPIC = "home/+/+/status"
ONLINE_TIMEOUT_SECONDS = int(os.getenv("ONLINE_TIMEOUT_SECONDS", "90"))
ALARM_DEDUP_SECONDS = int(os.getenv("ALARM_DEDUP_SECONDS", str(15 * 60)))
CONSECUTIVE_LIMIT = int(os.getenv("CONSECUTIVE_LIMIT", "3"))
RAW_DATA_RETENTION_DAYS = int(os.getenv("RAW_DATA_RETENTION_DAYS", "30"))
CLEANUP_INTERVAL_SECONDS = int(os.getenv("CLEANUP_INTERVAL_SECONDS", "86400"))
BACKUP_DIR = Path(os.getenv("BACKUP_DIR", ROOT / "backups"))
API_KEY = os.getenv("API_KEY", "")
MAX_QUERY_RANGE_SECONDS = int(os.getenv("MAX_QUERY_RANGE_SECONDS", str(7 * 86400)))
CORS_ORIGINS = [item.strip() for item in os.getenv("CORS_ORIGINS", "*").split(",") if item.strip()]

app = FastAPI(title="宿舍空气质量监测 API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS or ["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class APIKeyMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Any) -> Any:
        if API_KEY and request.url.path.startswith("/api/"):
            if request.url.path != "/api/health" and request.headers.get("X-API-Key") != API_KEY:
                return JSONResponse({"detail": "invalid API key"}, status_code=401)
        return await call_next(request)


app.add_middleware(APIKeyMiddleware)
_db_lock = threading.Lock()
_mqtt_client: Any = None
_mqtt_connected = False
_cleanup_stop = threading.Event()
_cleanup_thread: threading.Thread | None = None
logger = logging.getLogger("air_monitor")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


class ReadingIn(BaseModel):
    device_id: str = Field(min_length=1, max_length=64)
    room: str = Field(min_length=1, max_length=64)
    ts: int | None = None
    temp: float | None = Field(default=None, ge=-20, le=60)
    humi: float | None = Field(default=None, ge=0, le=100)
    aq_raw: int | None = None
    aq_index: float | None = None
    co2_ppm: float | None = Field(default=None, ge=0, le=10000)
    co2_valid: bool = True
    sensor_status: str = Field(default="OK", max_length=20)
    lux: float | None = None
    rssi: int | None = None
    flag: str = Field(default="OK", max_length=20)


class DeviceCreate(BaseModel):
    device_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
    room: str = Field(min_length=1, max_length=64)
    name: str | None = Field(default=None, max_length=100)


class ThresholdUpdate(BaseModel):
    device_id: str = Field(min_length=1, max_length=64)
    metric: str = Field(min_length=1, max_length=32)
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


class DeviceUpdate(BaseModel):
    room: str | None = Field(default=None, min_length=1, max_length=64)
    name: str | None = Field(default=None, max_length=100)


class VentilationEventIn(BaseModel):
    device_id: str = Field(min_length=1, max_length=64)
    started_at: int | None = None
    ended_at: int | None = None
    note: str = Field(default="", max_length=200)


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
                co2_ppm REAL,
                co2_valid INTEGER NOT NULL DEFAULT 1,
                sensor_status TEXT NOT NULL DEFAULT 'OK',
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
                acked_by TEXT,
                recovered INTEGER NOT NULL DEFAULT 0,
                recovered_at INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_alarm_device_ts ON alarm(device_id, ts);
            CREATE INDEX IF NOT EXISTS idx_alarm_ack_ts ON alarm(acked, ts);
            CREATE TABLE IF NOT EXISTS alarm_state (
                device_id TEXT NOT NULL,
                metric TEXT NOT NULL,
                high_count INTEGER NOT NULL DEFAULT 0,
                recovery_count INTEGER NOT NULL DEFAULT 0,
                last_level TEXT,
                PRIMARY KEY(device_id, metric)
            );
            CREATE TABLE IF NOT EXISTS config (
                device_id TEXT PRIMARY KEY,
                sample_interval INTEGER NOT NULL DEFAULT 5,
                push_interval INTEGER NOT NULL DEFAULT 30
            );
            CREATE TABLE IF NOT EXISTS ventilation_event (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL,
                started_at INTEGER NOT NULL,
                ended_at INTEGER,
                note TEXT NOT NULL DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_ventilation_device_time
                ON ventilation_event(device_id, started_at);
            """
        )
        columns = {
            row["name"]
            for row in db.execute("PRAGMA table_info(reading)").fetchall()
        }
        if "co2_ppm" not in columns:
            db.execute("ALTER TABLE reading ADD COLUMN co2_ppm REAL")
        if "co2_valid" not in columns:
            db.execute(
                "ALTER TABLE reading ADD COLUMN co2_valid INTEGER NOT NULL DEFAULT 1"
            )
        if "sensor_status" not in columns:
            db.execute(
                "ALTER TABLE reading ADD COLUMN sensor_status TEXT NOT NULL DEFAULT 'OK'"
            )
        alarm_columns = {
            row["name"] for row in db.execute("PRAGMA table_info(alarm)").fetchall()
        }
        if "recovered" not in alarm_columns:
            db.execute(
                "ALTER TABLE alarm ADD COLUMN recovered INTEGER NOT NULL DEFAULT 0"
            )
        if "recovered_at" not in alarm_columns:
            db.execute("ALTER TABLE alarm ADD COLUMN recovered_at INTEGER")
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
            INSERT INTO reading(
                device_id, ts, temp, humi, aq_raw, aq_index,
                co2_ppm, co2_valid, sensor_status, lux, rssi, flag
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload.device_id,
                timestamp,
                payload.temp,
                payload.humi,
                payload.aq_raw,
                payload.aq_index,
                payload.co2_ppm,
                1 if payload.co2_valid else 0,
                payload.sensor_status,
                payload.lux,
                payload.rssi,
                payload.flag,
            ),
        )
        values = {
            "temp": payload.temp,
            "humi": payload.humi,
            "aq_index": payload.aq_index,
            "co2_ppm": payload.co2_ppm if payload.co2_valid else None,
        }
        defaults = {
            "temp": (29.0, None, 30.0),
            "humi": (70.0, None, 75.0),
            "aq_index": (1.5, None, 2.5),
            "co2_ppm": (1400.0, None, 1800.0),
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
            state = db.execute(
                "SELECT high_count, recovery_count FROM alarm_state WHERE device_id=? AND metric=?",
                (payload.device_id, metric),
            ).fetchone()
            high_count = state["high_count"] if state else 0
            recovery_count = state["recovery_count"] if state else 0
            recovery_value = warn_hi
            is_recovered = recovery_value is not None and value <= recovery_value
            if level:
                high_count += 1
                recovery_count = 0
            elif is_recovered:
                recovery_count += 1
                high_count = 0
            else:
                high_count = 0
                recovery_count = 0
            db.execute(
                """
                INSERT INTO alarm_state(device_id, metric, high_count, recovery_count, last_level)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(device_id, metric) DO UPDATE SET
                    high_count=excluded.high_count,
                    recovery_count=excluded.recovery_count,
                    last_level=excluded.last_level
                """,
                (payload.device_id, metric, high_count, recovery_count, level),
            )
            if recovery_count >= CONSECUTIVE_LIMIT:
                db.execute(
                    """
                    UPDATE alarm SET recovered=1, recovered_at=?
                    WHERE device_id=? AND metric=? AND recovered=0
                    """,
                    (timestamp, payload.device_id, metric),
                )
            if level and high_count >= CONSECUTIVE_LIMIT and not _recent_alarm_exists(
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
        if online:
            db.execute(
                """
                UPDATE alarm SET recovered=1, recovered_at=COALESCE(?, CAST(strftime('%s','now') AS INTEGER))
                WHERE device_id=? AND metric='device' AND recovered=0
                """,
                (timestamp, device_id),
            )
        db.commit()


def mqtt_message(_client: Any, _userdata: Any, message: Any) -> None:
    try:
        payload = ReadingIn.model_validate(json.loads(message.payload.decode("utf-8")))
        save_reading(payload)
    except Exception as exc:
        logger.warning("ignored invalid MQTT payload: %s", exc)


def mqtt_status_message(_client: Any, _userdata: Any, message: Any) -> None:
    parts = message.topic.split("/")
    if len(parts) != 4 or parts[0] != "home" or parts[3] != "status":
        return
    device_id = parts[2]
    status = message.payload.decode("utf-8", errors="replace").strip().lower()
    update_device_status(device_id, status == "online")


def mqtt_connected(
    _client: Any, _userdata: Any, _flags: Any, reason_code: Any, _properties: Any
) -> None:
    global _mqtt_connected
    _mqtt_connected = int(reason_code) == 0
    if _mqtt_connected:
        logger.info("MQTT connected to %s:%s", MQTT_HOST, MQTT_PORT)
    else:
        logger.error("MQTT connection rejected: %s", reason_code)


def mqtt_disconnected(
    _client: Any, _userdata: Any, _disconnect_flags: Any, reason_code: Any, _properties: Any
) -> None:
    global _mqtt_connected
    _mqtt_connected = False
    logger.warning("MQTT disconnected: %s", reason_code)


def start_mqtt() -> None:
    global _mqtt_client
    if not MQTT_HOST or mqtt is None:
        logger.info("MQTT disabled; using REST-only mode")
        return
    _mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    _mqtt_client.on_message = mqtt_message
    _mqtt_client.on_connect = mqtt_connected
    _mqtt_client.on_disconnect = mqtt_disconnected
    if MQTT_USERNAME:
        _mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    if MQTT_TLS:
        _mqtt_client.tls_set()
    _mqtt_client.message_callback_add(MQTT_STATUS_TOPIC, mqtt_status_message)
    _mqtt_client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
    _mqtt_client.subscribe([(MQTT_TOPIC, 1), (MQTT_STATUS_TOPIC, 1)])
    _mqtt_client.loop_start()


def cleanup_old_data() -> int:
    cutoff = int(time.time()) - RAW_DATA_RETENTION_DAYS * 86400
    with _db_lock, closing(connect_db()) as db:
        cursor = db.execute("DELETE FROM reading WHERE ts < ?", (cutoff,))
        db.execute("DELETE FROM alarm WHERE ts < ?", (cutoff,))
        db.execute(
            "DELETE FROM alarm_state WHERE device_id NOT IN (SELECT device_id FROM device)"
        )
        db.commit()
    logger.info("cleaned %s readings and old alarms before %s", cursor.rowcount, cutoff)
    return cursor.rowcount


@app.post("/api/maintenance/cleanup")
def run_cleanup() -> dict[str, Any]:
    return {"deleted_readings": cleanup_old_data()}


def backup_database() -> Path:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    target = BACKUP_DIR / f"air_monitor_{int(time.time())}.db"
    with _db_lock, closing(connect_db()) as source:
        destination = sqlite3.connect(target)
        try:
            source.backup(destination)
        finally:
            destination.close()
    logger.info("database backup created: %s", target)
    return target


@app.post("/api/maintenance/backup")
def run_backup() -> dict[str, Any]:
    target = backup_database()
    return {"created": True, "file": str(target)}


@app.get("/api/maintenance/backups")
def list_backups(limit: int = Query(20, ge=1, le=100)) -> list[dict[str, Any]]:
    if not isinstance(limit, int):
        limit = 20
    if not BACKUP_DIR.exists():
        return []
    files = sorted(
        (path for path in BACKUP_DIR.glob("air_monitor_*.db") if path.is_file()),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )[:limit]
    return [
        {
            "file": str(path),
            "name": path.name,
            "size": path.stat().st_size,
            "modified_at": int(path.stat().st_mtime),
        }
        for path in files
    ]


def cleanup_loop() -> None:
    while not _cleanup_stop.wait(CLEANUP_INTERVAL_SECONDS):
        try:
            cleanup_old_data()
        except sqlite3.Error:
            logger.exception("scheduled data cleanup failed")


def start_cleanup_task() -> None:
    global _cleanup_thread
    _cleanup_stop.clear()
    cleanup_old_data()
    _cleanup_thread = threading.Thread(
        target=cleanup_loop, name="air-data-cleanup", daemon=True
    )
    _cleanup_thread.start()


def stop_cleanup_task() -> None:
    _cleanup_stop.set()
    if _cleanup_thread is not None:
        _cleanup_thread.join(timeout=2)


@app.on_event("startup")
def startup() -> None:
    init_db()
    start_mqtt()
    start_cleanup_task()


@app.on_event("shutdown")
def shutdown() -> None:
    global _mqtt_connected
    stop_cleanup_task()
    if _mqtt_client is not None:
        _mqtt_client.loop_stop()
        _mqtt_client.disconnect()
    _mqtt_connected = False


@app.get("/api/health")
def health() -> dict[str, Any]:
    try:
        with closing(connect_db()) as db:
            db.execute("SELECT 1").fetchone()
        db_status = "ok"
    except sqlite3.Error:
        db_status = "error"
    status = "ok" if db_status == "ok" else "degraded"
    return {
        "status": status,
        "mqtt": _mqtt_connected,
        "mqtt_configured": bool(MQTT_HOST),
        "mqtt_tls": MQTT_TLS,
        "database": db_status,
        "api_key_enabled": bool(API_KEY),
        "db": str(DB_PATH),
    }


@app.post("/api/readings")
def create_reading(payload: ReadingIn) -> dict[str, Any]:
    try:
        return save_reading(payload)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


@app.get("/api/devices")
def devices() -> list[dict[str, Any]]:
    with closing(connect_db()) as db:
        stale_before = int(time.time()) - ONLINE_TIMEOUT_SECONDS
        stale = db.execute(
            """
            SELECT device_id, last_seen FROM device
            WHERE online=1 AND last_seen IS NOT NULL AND last_seen<?
            """,
            (stale_before,),
        ).fetchall()
        for row in stale:
            if not _recent_alarm_exists(db, row["device_id"], "device", "L3", int(time.time())):
                db.execute(
                    """
                    INSERT INTO alarm(device_id, metric, level, value, ts)
                    VALUES (?, 'device', 'L3', ?, ?)
                    """,
                    (row["device_id"], int(time.time()) - row["last_seen"], int(time.time())),
                )
        if stale:
            db.execute(
                "UPDATE device SET online=0 WHERE online=1 AND last_seen<?",
                (stale_before,),
            )
        db.commit()
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


@app.post("/api/devices", status_code=201)
def create_device(payload: DeviceCreate) -> dict[str, Any]:
    with _db_lock, closing(connect_db()) as db:
        try:
            db.execute(
                "INSERT INTO device(device_id, room, name) VALUES (?, ?, ?)",
                (payload.device_id, payload.room, payload.name),
            )
        except sqlite3.IntegrityError as exc:
            raise HTTPException(409, "device already exists") from exc
        db.commit()
    return {"created": True, **payload.model_dump(), "online": 0}


@app.post("/api/devices/{device_id}/online")
def mark_device_online(device_id: str) -> dict[str, Any]:
    with closing(connect_db()) as db:
        if db.execute(
            "SELECT 1 FROM device WHERE device_id=?", (device_id,)
        ).fetchone() is None:
            raise HTTPException(404, "device not found")
    timestamp = int(time.time())
    update_device_status(device_id, True, timestamp)
    return {"updated": True, "device_id": device_id, "online": True, "last_seen": timestamp}


@app.post("/api/devices/{device_id}/offline")
def mark_device_offline(device_id: str) -> dict[str, Any]:
    with closing(connect_db()) as db:
        if db.execute(
            "SELECT 1 FROM device WHERE device_id=?", (device_id,)
        ).fetchone() is None:
            raise HTTPException(404, "device not found")
    update_device_status(device_id, False)
    return {"updated": True, "device_id": device_id, "online": False}


@app.get("/api/devices/{device_id}")
def device_detail(device_id: str) -> dict[str, Any]:
    with closing(connect_db()) as db:
        device = db.execute(
            """
            SELECT device_id, room, name, last_seen,
                   CASE WHEN last_seen IS NOT NULL AND last_seen>=?
                        THEN online ELSE 0 END AS online
            FROM device WHERE device_id=?
            """,
            (int(time.time()) - ONLINE_TIMEOUT_SECONDS, device_id),
        ).fetchone()
        if device is None:
            raise HTTPException(404, "device not found")
        reading_count = db.execute(
            "SELECT COUNT(*) AS count FROM reading WHERE device_id=?",
            (device_id,),
        ).fetchone()["count"]
        alarm_count = db.execute(
            "SELECT COUNT(*) AS count FROM alarm WHERE device_id=? AND acked=0",
            (device_id,),
        ).fetchone()["count"]
    return {
        **dict(device),
        "reading_count": reading_count,
        "unacked_alarm_count": alarm_count,
    }


@app.put("/api/devices/{device_id}")
def update_device(device_id: str, payload: DeviceUpdate) -> dict[str, Any]:
    if payload.room is None and payload.name is None:
        raise HTTPException(400, "room or name is required")
    with _db_lock, closing(connect_db()) as db:
        current = db.execute(
            "SELECT device_id, room, name FROM device WHERE device_id=?",
            (device_id,),
        ).fetchone()
        if current is None:
            raise HTTPException(404, "device not found")
        room = payload.room if payload.room is not None else current["room"]
        name = payload.name if payload.name is not None else current["name"]
        db.execute(
            "UPDATE device SET room=?, name=? WHERE device_id=?",
            (room, name, device_id),
        )
        db.commit()
    return {"updated": True, "device_id": device_id, "room": room, "name": name}


@app.delete("/api/devices/{device_id}")
def delete_device(device_id: str, delete_readings: bool = False) -> dict[str, Any]:
    with _db_lock, closing(connect_db()) as db:
        exists = db.execute(
            "SELECT 1 FROM device WHERE device_id=?", (device_id,)
        ).fetchone()
        if exists is None:
            raise HTTPException(404, "device not found")
        if delete_readings:
            db.execute("DELETE FROM reading WHERE device_id=?", (device_id,))
            db.execute("DELETE FROM alarm WHERE device_id=?", (device_id,))
            db.execute(
                "DELETE FROM ventilation_event WHERE device_id=?", (device_id,)
            )
        else:
            reading_count = db.execute(
                "SELECT COUNT(*) AS count FROM reading WHERE device_id=?",
                (device_id,),
            ).fetchone()["count"]
            if reading_count:
                raise HTTPException(
                    409,
                    "device has readings; pass delete_readings=true to delete history",
                )
        db.execute("DELETE FROM threshold WHERE device_id=?", (device_id,))
        db.execute("DELETE FROM config WHERE device_id=?", (device_id,))
        db.execute("DELETE FROM device WHERE device_id=?", (device_id,))
        db.commit()
    return {"deleted": True, "device_id": device_id}


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
    metric: str = Query(
        "aq_index", pattern="^(temp|humi|aq_raw|aq_index|co2_ppm|lux)$"
    ),
    from_ts: int | None = Query(None, alias="from"),
    to_ts: int | None = Query(None, alias="to"),
    bucket: str = Query("raw", pattern="^(raw|1m|1h)$"),
) -> list[dict[str, Any]]:
    allowed = {"temp", "humi", "aq_raw", "aq_index", "co2_ppm", "lux"}
    if metric not in allowed:
        raise HTTPException(400, "unsupported metric")
    start = from_ts if from_ts is not None else int(time.time()) - 24 * 3600
    end = to_ts if to_ts is not None else int(time.time())
    if end - start > MAX_QUERY_RANGE_SECONDS:
        raise HTTPException(400, "query range exceeds configured limit")
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


@app.get("/api/readings/page")
def readings_page(
    device_id: str | None = None,
    from_ts: int | None = Query(None, alias="from"),
    to_ts: int | None = Query(None, alias="to"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> list[dict[str, Any]]:
    if not isinstance(limit, int):
        limit = 100
    if not isinstance(offset, int):
        offset = 0
    if not isinstance(from_ts, int):
        from_ts = None
    if not isinstance(to_ts, int):
        to_ts = None
    if from_ts is not None and to_ts is not None and from_ts > to_ts:
        raise HTTPException(400, "from must be less than or equal to to")
    if (
        from_ts is not None
        and to_ts is not None
        and to_ts - from_ts > MAX_QUERY_RANGE_SECONDS
    ):
        raise HTTPException(400, "query range exceeds configured limit")
    clauses = ["1=1"]
    params: list[Any] = []
    if device_id:
        clauses.append("r.device_id=?")
        params.append(device_id)
    if from_ts is not None:
        clauses.append("r.ts>=?")
        params.append(from_ts)
    if to_ts is not None:
        clauses.append("r.ts<=?")
        params.append(to_ts)
    params.extend((limit, offset))
    with closing(connect_db()) as db:
        rows = db.execute(
            f"""
            SELECT r.*, d.room, d.name AS device_name
            FROM reading r JOIN device d USING(device_id)
            WHERE {' AND '.join(clauses)}
            ORDER BY r.ts DESC, r.id DESC
            LIMIT ? OFFSET ?
            """,
            params,
        ).fetchall()
    return [dict(row) for row in rows]


@app.put("/api/threshold")
def update_threshold(payload: ThresholdUpdate) -> dict[str, Any]:
    if payload.metric not in {"temp", "humi", "aq_index", "co2_ppm", "aq_raw", "lux"}:
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


@app.get("/api/thresholds")
def thresholds(device_id: str | None = None) -> list[dict[str, Any]]:
    with closing(connect_db()) as db:
        if device_id:
            rows = db.execute(
                "SELECT * FROM threshold WHERE device_id=? ORDER BY metric",
                (device_id,),
            ).fetchall()
        else:
            rows = db.execute("SELECT * FROM threshold ORDER BY device_id, metric").fetchall()
    return [dict(row) for row in rows]


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
    return alarm_page(device_id=device_id, limit=limit)


@app.get("/api/alarms/page")
def alarm_page(
    device_id: str | None = None,
    acked: bool | None = None,
    from_ts: int | None = Query(None, alias="from"),
    to_ts: int | None = Query(None, alias="to"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> list[dict[str, Any]]:
    if not isinstance(from_ts, int):
        from_ts = None
    if not isinstance(to_ts, int):
        to_ts = None
    if not isinstance(limit, int):
        limit = 50
    if not isinstance(offset, int):
        offset = 0
    if from_ts is not None and to_ts is not None and from_ts > to_ts:
        raise HTTPException(400, "from must be less than or equal to to")
    if (
        from_ts is not None
        and to_ts is not None
        and to_ts - from_ts > MAX_QUERY_RANGE_SECONDS
    ):
        raise HTTPException(400, "query range exceeds configured limit")
    clauses = ["1=1"]
    params: list[Any] = []
    if device_id:
        clauses.append("device_id=?")
        params.append(device_id)
    if acked is not None:
        clauses.append("acked=?")
        params.append(1 if acked else 0)
    if from_ts is not None:
        clauses.append("ts>=?")
        params.append(from_ts)
    if to_ts is not None:
        clauses.append("ts<=?")
        params.append(to_ts)
    params.extend((limit, offset))
    with closing(connect_db()) as db:
        rows = db.execute(
            f"""
            SELECT * FROM alarm
            WHERE {' AND '.join(clauses)}
            ORDER BY ts DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            params,
        ).fetchall()
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


@app.get("/api/summary")
def summary() -> dict[str, Any]:
    device_rows = devices()
    alarm_rows = alarms(limit=500)
    latest_rows = latest()
    return {
        "device_count": len(device_rows),
        "online_count": sum(1 for row in device_rows if row["online"]),
        "unacked_alarm_count": sum(1 for row in alarm_rows if not row["acked"]),
        "latest": latest_rows,
        "generated_at": int(time.time()),
    }


@app.get("/api/statistics")
def statistics(
    device_id: str | None = None,
    metric: str = Query(
        "co2_ppm", pattern="^(temp|humi|aq_index|co2_ppm|lux)$"
    ),
    from_ts: int | None = Query(None, alias="from"),
    to_ts: int | None = Query(None, alias="to"),
) -> dict[str, Any]:
    start = from_ts if from_ts is not None else int(time.time()) - 24 * 3600
    end = to_ts if to_ts is not None else int(time.time())
    if start > end:
        raise HTTPException(400, "from must be less than or equal to to")
    clauses = [f"{metric} IS NOT NULL", "ts BETWEEN ? AND ?"]
    params: list[Any] = [start, end]
    if device_id:
        clauses.append("device_id=?")
        params.append(device_id)
    with closing(connect_db()) as db:
        row = db.execute(
            f"""
            SELECT COUNT(*) AS count, AVG({metric}) AS average,
                   MIN({metric}) AS minimum, MAX({metric}) AS maximum
            FROM reading WHERE {' AND '.join(clauses)}
            """,
            params,
        ).fetchone()
    return {
        "device_id": device_id,
        "metric": metric,
        "from": start,
        "to": end,
        "count": row["count"],
        "average": row["average"],
        "minimum": row["minimum"],
        "maximum": row["maximum"],
    }


@app.get("/api/statistics/alarms")
def alarm_statistics(
    device_id: str | None = None,
    from_ts: int | None = Query(None, alias="from"),
    to_ts: int | None = Query(None, alias="to"),
) -> dict[str, Any]:
    start = from_ts if from_ts is not None else int(time.time()) - 24 * 3600
    end = to_ts if to_ts is not None else int(time.time())
    if start > end:
        raise HTTPException(400, "from must be less than or equal to to")
    clauses = ["ts BETWEEN ? AND ?"]
    params: list[Any] = [start, end]
    if device_id:
        clauses.append("device_id=?")
        params.append(device_id)
    with closing(connect_db()) as db:
        rows = db.execute(
            f"""
            SELECT level, COUNT(*) AS count
            FROM alarm WHERE {' AND '.join(clauses)}
            GROUP BY level ORDER BY level
            """,
            params,
        ).fetchall()
        totals = db.execute(
            f"""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN acked=1 THEN 1 ELSE 0 END) AS acked,
                   SUM(CASE WHEN recovered=1 THEN 1 ELSE 0 END) AS recovered
            FROM alarm WHERE {' AND '.join(clauses)}
            """,
            params,
        ).fetchone()
    return {
        "device_id": device_id,
        "from": start,
        "to": end,
        "total": totals["total"],
        "acked": totals["acked"] or 0,
        "recovered": totals["recovered"] or 0,
        "by_level": [dict(row) for row in rows],
    }


@app.get("/api/export.csv")
def export_csv(
    device_id: str | None = None,
    from_ts: int | None = Query(None, alias="from"),
    to_ts: int | None = Query(None, alias="to"),
) -> StreamingResponse:
    start = from_ts if from_ts is not None else int(time.time()) - 24 * 3600
    end = to_ts if to_ts is not None else int(time.time())
    if start > end:
        raise HTTPException(400, "from must be less than or equal to to")
    with closing(connect_db()) as db:
        if device_id:
            rows = db.execute(
                """
                SELECT r.*, d.room FROM reading r
                JOIN device d USING(device_id)
                WHERE r.device_id=? AND r.ts BETWEEN ? AND ?
                ORDER BY r.ts
                """,
                (device_id, start, end),
            ).fetchall()
        else:
            rows = db.execute(
                """
                SELECT r.*, d.room FROM reading r
                JOIN device d USING(device_id)
                WHERE r.ts BETWEEN ? AND ?
                ORDER BY r.ts
                """,
                (start, end),
            ).fetchall()
    fields = [
        "id", "device_id", "room", "ts", "temp", "humi", "aq_raw",
        "aq_index", "co2_ppm", "co2_valid", "sensor_status", "lux",
        "rssi", "flag",
    ]
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=fields)
    writer.writeheader()
    for row in rows:
        writer.writerow({field: row[field] for field in fields})
    content = output.getvalue().encode("utf-8-sig")
    return StreamingResponse(
        iter([content]),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": 'attachment; filename="air-readings.csv"'
        },
    )


@app.get("/api/ventilation-events")
def ventilation_events(
    device_id: str | None = None,
    limit: int = Query(50, ge=1, le=500),
) -> list[dict[str, Any]]:
    if not isinstance(limit, int):
        limit = 50
    with closing(connect_db()) as db:
        if device_id:
            rows = db.execute(
                """
                SELECT * FROM ventilation_event
                WHERE device_id=? ORDER BY started_at DESC LIMIT ?
                """,
                (device_id, limit),
            ).fetchall()
        else:
            rows = db.execute(
                "SELECT * FROM ventilation_event ORDER BY started_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
    return [dict(row) for row in rows]


@app.get("/api/ventilation-events/{event_id}/analysis")
def ventilation_analysis(event_id: int) -> dict[str, Any]:
    with closing(connect_db()) as db:
        event = db.execute(
            "SELECT * FROM ventilation_event WHERE id=?", (event_id,)
        ).fetchone()
        if event is None:
            raise HTTPException(404, "ventilation event not found")
        end = event["ended_at"] or event["started_at"]
        before = db.execute(
            """
            SELECT co2_ppm FROM reading
            WHERE device_id=? AND co2_valid=1 AND ts<=? AND co2_ppm IS NOT NULL
            ORDER BY ts DESC LIMIT 1
            """,
            (event["device_id"], event["started_at"]),
        ).fetchone()
        after = db.execute(
            """
            SELECT co2_ppm FROM reading
            WHERE device_id=? AND co2_valid=1 AND ts>=? AND co2_ppm IS NOT NULL
            ORDER BY ts ASC LIMIT 1
            """,
            (event["device_id"], end),
        ).fetchone()
    before_co2 = before["co2_ppm"] if before else None
    after_co2 = after["co2_ppm"] if after else None
    drop = before_co2 - after_co2 if before_co2 is not None and after_co2 is not None else None
    duration = end - event["started_at"] if event["ended_at"] is not None else None
    return {
        "event_id": event_id,
        "device_id": event["device_id"],
        "before_co2": before_co2,
        "after_co2": after_co2,
        "drop_ppm": drop,
        "duration_seconds": duration,
        "effective": drop is not None and drop >= 100,
    }


@app.post("/api/ventilation-events")
def create_ventilation_event(payload: VentilationEventIn) -> dict[str, Any]:
    started_at = payload.started_at or int(time.time())
    ended_at = payload.ended_at
    if ended_at is not None and ended_at < started_at:
        raise HTTPException(400, "ended_at must be greater than or equal to started_at")
    with _db_lock, closing(connect_db()) as db:
        device = db.execute(
            "SELECT 1 FROM device WHERE device_id=?", (payload.device_id,)
        ).fetchone()
        if device is None:
            raise HTTPException(404, "device not found")
        cursor = db.execute(
            """
            INSERT INTO ventilation_event(device_id, started_at, ended_at, note)
            VALUES (?, ?, ?, ?)
            """,
            (payload.device_id, started_at, ended_at, payload.note),
        )
        db.commit()
        event_id = cursor.lastrowid
    return {
        "created": True,
        "id": event_id,
        "device_id": payload.device_id,
        "started_at": started_at,
        "ended_at": ended_at,
    }


WEB_ROOT = ROOT.parent / "web"
if WEB_ROOT.exists():
    app.mount("/", StaticFiles(directory=WEB_ROOT, html=True), name="web")
