import tempfile
import unittest
from contextlib import closing
from pathlib import Path

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
        backend.save_reading(payload)
        backend.save_reading(payload.model_copy(update={"ts": 1030}))
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


if __name__ == "__main__":
    unittest.main()
