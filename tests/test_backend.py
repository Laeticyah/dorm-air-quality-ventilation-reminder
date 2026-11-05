import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from fastapi.testclient import TestClient

from backend import app as backend


class BackendTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        backend.DB_PATH = Path(self.temp_dir.name) / "air.db"
        backend.init_db()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_reading_creates_device_and_deduplicates_alarm(self) -> None:
        payload = backend.ReadingIn(
            device_id="esp32-a1",
            room="dorm-302",
            ts=1000,
            temp=31,
            humi=80,
            aq_index=3.0,
        )
        for timestamp in (1000, 1030, 1060):
            backend.save_reading(payload.model_copy(update={"ts": timestamp}))
        with closing(backend.connect_db()) as db:
            devices = db.execute("SELECT device_id, online FROM device").fetchall()
            alarms = db.execute("SELECT metric, level FROM alarm").fetchall()
        self.assertEqual([(row["device_id"], row["online"]) for row in devices], [("esp32-a1", 1)])
        self.assertEqual(len(alarms), 3)
        self.assertTrue(all(row["level"] == "L3" for row in alarms))

    def test_history_can_aggregate_by_minute(self) -> None:
        for timestamp, value in ((60, 1.0), (90, 2.0), (120, 4.0)):
            backend.save_reading(
                backend.ReadingIn(
                    device_id="esp32-a1",
                    room="dorm-302",
                    ts=timestamp,
                    aq_index=value,
                )
            )
        rows = backend.history("esp32-a1", metric="aq_index", from_ts=0, to_ts=200, bucket="1m")
        self.assertEqual(rows[0]["ts"], 60)
        self.assertAlmostEqual(rows[0]["value"], 1.5)
        self.assertEqual(rows[1]["ts"], 120)

    def test_stale_device_is_reported_offline(self) -> None:
        backend.save_reading(
            backend.ReadingIn(
                device_id="esp32-a1",
                room="dorm-302",
                ts=100,
                temp=24,
            )
        )
        rows = backend.devices()
        self.assertEqual(rows[0]["online"], 0)

    def test_thresholds_can_be_updated_and_read(self) -> None:
        payload = backend.ThresholdUpdate(
            device_id="esp32-a1",
            metric="temp",
            warn_hi=28.5,
            alert_hi=30.0,
        )
        self.assertEqual(backend.update_threshold(payload)["updated"], True)
        rows = backend.thresholds("esp32-a1")
        self.assertEqual(rows[0]["metric"], "temp")
        self.assertEqual(rows[0]["warn_hi"], 28.5)

    def test_co2_reading_generates_alarm_and_is_available_in_history(self) -> None:
        for timestamp in (1000, 1030, 1060):
            backend.save_reading(
                backend.ReadingIn(
                    device_id="esp32-a1",
                    room="dorm-302",
                    ts=timestamp,
                    co2_ppm=1500,
                    co2_valid=True,
                )
            )
        with closing(backend.connect_db()) as db:
            row = db.execute(
                "SELECT co2_ppm, co2_valid FROM reading"
            ).fetchone()
            alarm = db.execute(
                "SELECT metric, level FROM alarm WHERE metric='co2_ppm'"
            ).fetchone()
        self.assertEqual(row["co2_ppm"], 1500)
        self.assertEqual(row["co2_valid"], 1)
        self.assertEqual((alarm["metric"], alarm["level"]), ("co2_ppm", "L2"))

    def test_ventilation_event_can_be_created_and_listed(self) -> None:
        backend.save_reading(
            backend.ReadingIn(
                device_id="esp32-a1",
                room="dorm-302",
                ts=1000,
                co2_ppm=1200,
            )
        )
        event = backend.create_ventilation_event(
            backend.VentilationEventIn(
                device_id="esp32-a1",
                started_at=1010,
                ended_at=1090,
                note="开窗通风",
            )
        )
        rows = backend.ventilation_events(device_id="esp32-a1")
        self.assertTrue(event["created"])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["note"], "开窗通风")

    def test_export_contains_co2_columns(self) -> None:
        backend.save_reading(
            backend.ReadingIn(
                device_id="esp32-a1",
                room="dorm-302",
                ts=1000,
                co2_ppm=900,
            )
        )
        client = TestClient(backend.app)
        response = client.get("/api/export.csv?from=900&to=1100")
        self.assertEqual(response.status_code, 200)
        self.assertIn("co2_ppm", response.text)
        self.assertIn("900", response.text)

    def test_device_detail_and_update(self) -> None:
        backend.save_reading(
            backend.ReadingIn(
                device_id="esp32-a1",
                room="dorm-302",
                ts=1000,
                temp=24,
            )
        )
        updated = backend.update_device(
            "esp32-a1",
            backend.DeviceUpdate(name="模拟设备", room="dorm-303"),
        )
        detail = backend.device_detail("esp32-a1")
        self.assertEqual(updated["room"], "dorm-303")
        self.assertEqual(detail["name"], "模拟设备")
        self.assertEqual(detail["reading_count"], 1)

    def test_alarm_page_supports_ack_filter_and_offset(self) -> None:
        for timestamp, temp, humi in (
            (1000, 31, None),
            (1010, 31, None),
            (1020, 31, None),
            (1030, None, 80),
            (1040, None, 80),
            (1050, None, 80),
        ):
            backend.save_reading(
                backend.ReadingIn(
                    device_id="esp32-a1",
                    room="dorm-302",
                    ts=timestamp,
                    temp=temp,
                    humi=humi,
                )
            )
        backend.acknowledge_alarm(1, backend.AlarmAck(acked_by="tester"))
        rows = backend.alarm_page(device_id="esp32-a1", acked=False, limit=1, offset=0)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["acked"], 0)

    def test_device_registration_pagination_and_safe_delete(self) -> None:
        created = backend.create_device(
            backend.DeviceCreate(
                device_id="esp32-b2",
                room="dorm-303",
                name="备用设备",
            )
        )
        self.assertTrue(created["created"])
        backend.save_reading(
            backend.ReadingIn(
                device_id="esp32-b2",
                room="dorm-303",
                ts=1000,
                co2_ppm=700,
            )
        )
        rows = backend.readings_page(device_id="esp32-b2", limit=1, offset=0)
        self.assertEqual(len(rows), 1)
        with self.assertRaises(Exception):
            backend.delete_device("esp32-b2")
        result = backend.delete_device("esp32-b2", delete_readings=True)
        self.assertTrue(result["deleted"])

    def test_alarm_is_marked_recovered_after_returning_below_warn_level(self) -> None:
        for timestamp in (1000, 1010, 1020):
            backend.save_reading(
                backend.ReadingIn(
                    device_id="esp32-a1",
                    room="dorm-302",
                    ts=timestamp,
                    co2_ppm=1500,
                )
            )
        for timestamp in (1100, 1110, 1120):
            backend.save_reading(
                backend.ReadingIn(
                    device_id="esp32-a1",
                    room="dorm-302",
                    ts=timestamp,
                    co2_ppm=900,
                )
            )
        with closing(backend.connect_db()) as db:
            alarm = db.execute(
                "SELECT recovered, recovered_at FROM alarm WHERE metric='co2_ppm'"
            ).fetchone()
        self.assertEqual(alarm["recovered"], 1)
        self.assertEqual(alarm["recovered_at"], 1120)

    def test_statistics_returns_metric_summary(self) -> None:
        for timestamp, co2 in ((1000, 800), (1010, 1200), (1020, 1600)):
            backend.save_reading(
                backend.ReadingIn(
                    device_id="esp32-a1",
                    room="dorm-302",
                    ts=timestamp,
                    co2_ppm=co2,
                )
            )
        result = backend.statistics(
            device_id="esp32-a1",
            metric="co2_ppm",
            from_ts=900,
            to_ts=1100,
        )
        self.assertEqual(result["count"], 3)
        self.assertEqual(result["minimum"], 800)
        self.assertEqual(result["maximum"], 1600)
        self.assertAlmostEqual(result["average"], 1200)

    def test_ventilation_analysis_calculates_co2_drop(self) -> None:
        backend.save_reading(
            backend.ReadingIn(
                device_id="esp32-a1",
                room="dorm-302",
                ts=1000,
                co2_ppm=1700,
            )
        )
        backend.save_reading(
            backend.ReadingIn(
                device_id="esp32-a1",
                room="dorm-302",
                ts=1600,
                co2_ppm=900,
            )
        )
        event = backend.create_ventilation_event(
            backend.VentilationEventIn(
                device_id="esp32-a1",
                started_at=1010,
                ended_at=1600,
            )
        )
        result = backend.ventilation_analysis(event["id"])
        self.assertEqual(result["before_co2"], 1700)
        self.assertEqual(result["after_co2"], 900)
        self.assertEqual(result["drop_ppm"], 800)
        self.assertTrue(result["effective"])

    def test_database_backup_creates_file(self) -> None:
        backup_dir = Path(self.temp_dir.name) / "backups"
        original = backend.BACKUP_DIR
        backend.BACKUP_DIR = backup_dir
        try:
            target = backend.backup_database()
            self.assertTrue(target.exists())
            self.assertGreater(target.stat().st_size, 0)
        finally:
            backend.BACKUP_DIR = original

    def test_health_reports_database_and_mqtt_configuration(self) -> None:
        result = backend.health()
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["database"], "ok")
        self.assertFalse(result["mqtt"])
        self.assertIn("mqtt_configured", result)


if __name__ == "__main__":
    unittest.main()
