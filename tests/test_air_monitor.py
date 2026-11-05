import unittest

from src.air_monitor import AirMonitor, AirState, SensorData


def data(timestamp: int, co2: float | None, valid: bool = True) -> SensorData:
    return SensorData(
        timestamp=timestamp,
        co2=co2,
        temperature=24.0,
        humidity=60.0,
        co2_valid=valid,
    )


class AirMonitorTests(unittest.TestCase):
    def test_requires_three_consecutive_high_samples(self) -> None:
        monitor = AirMonitor(warmup_seconds=0, filter_size=1)
        states = [monitor.process(data(i * 30, 1100)).state for i in range(3)]
        self.assertEqual(states[:2], [AirState.ATTENTION, AirState.ATTENTION])
        self.assertEqual(states[2], AirState.VENTILATE)

    def test_high_co2_is_escalated(self) -> None:
        monitor = AirMonitor(warmup_seconds=0, filter_size=1)
        for i in range(3):
            status = monitor.process(data(i * 30, 1500))
        self.assertEqual(status.state, AirState.HIGH)
        self.assertTrue(status.buzzer)

    def test_mute_suppresses_buzzer(self) -> None:
        monitor = AirMonitor(warmup_seconds=0, filter_size=1)
        monitor.process(data(0, 1500))
        monitor.process(data(30, 1500))
        status = monitor.process(data(60, 1500))
        self.assertTrue(status.buzzer)
        monitor.mute(60)
        status = monitor.process(data(90, 1500))
        self.assertFalse(status.buzzer)
        self.assertTrue(status.muted)

    def test_window_recovery_enters_recovering_state(self) -> None:
        monitor = AirMonitor(warmup_seconds=0, filter_size=1)
        for i in range(3):
            monitor.process(data(i * 30, 1200))
        status = monitor.process(data(120, 800))
        self.assertEqual(status.state, AirState.RECOVERING)

    def test_invalid_co2_produces_sensor_error(self) -> None:
        monitor = AirMonitor(warmup_seconds=0, filter_size=1)
        status = monitor.process(data(0, None, valid=False))
        self.assertEqual(status.state, AirState.SENSOR_ERROR)
        self.assertTrue(status.buzzer)


if __name__ == "__main__":
    unittest.main()
