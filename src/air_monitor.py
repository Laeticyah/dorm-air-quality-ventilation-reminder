from __future__ import annotations

import csv
from collections import deque
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional, TextIO


@dataclass(frozen=True)
class Thresholds:
    co2_attention: float = 1000
    co2_high: float = 1400
    co2_critical: float = 1800
    co2_recover: float = 950
    humidity_attention: float = 65
    humidity_high: float = 75
    pm25_attention: float = 35
    pm25_high: float = 75
    required_over_limit_samples: int = 3
    required_normal_samples: int = 10
    reminder_interval_seconds: int = 15 * 60
    recovery_hold_seconds: int = 5 * 60


@dataclass
class SensorData:
    timestamp: int
    co2: Optional[float]
    temperature: Optional[float]
    humidity: Optional[float]
    pm25: Optional[float] = None
    co2_valid: bool = True
    temperature_valid: bool = True
    humidity_valid: bool = True
    pm25_valid: bool = True


class AirState(str, Enum):
    WARMUP = "WARMUP"
    NORMAL = "NORMAL"
    ATTENTION = "ATTENTION"
    VENTILATE = "VENTILATE"
    HIGH = "HIGH"
    RECOVERING = "RECOVERING"
    SENSOR_ERROR = "SENSOR_ERROR"


class ReminderLevel(str, Enum):
    NONE = "NONE"
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


@dataclass
class AirStatus:
    state: AirState
    reminder: ReminderLevel
    message: str
    led: str
    buzzer: bool
    muted: bool
    delayed: bool
    filtered: SensorData


class MovingAverage:
    def __init__(self, size: int = 5) -> None:
        if size <= 0:
            raise ValueError("size must be positive")
        self._values: deque[float] = deque(maxlen=size)

    def update(self, value: float) -> float:
        self._values.append(value)
        return sum(self._values) / len(self._values)

    @property
    def count(self) -> int:
        return len(self._values)


class AirMonitor:
    """Hardware-independent air-quality state machine.

    The monitor consumes SensorData and produces an AirStatus. Real sensors,
    a simulator, and future network inputs can all use the same interface.
    """

    def __init__(
        self,
        thresholds: Thresholds | None = None,
        warmup_seconds: int = 180,
        filter_size: int = 5,
    ) -> None:
        self.thresholds = thresholds or Thresholds()
        self.warmup_seconds = warmup_seconds
        self._started_at: Optional[int] = None
        self._co2_filter = MovingAverage(filter_size)
        self._temp_filter = MovingAverage(filter_size)
        self._humidity_filter = MovingAverage(filter_size)
        self._pm25_filter = MovingAverage(filter_size)
        self._state = AirState.WARMUP
        self._over_limit_count = 0
        self._normal_count = 0
        self._recovering_since: Optional[int] = None
        self._muted_until = 0
        self._delayed_until = 0
        self._last_reminder_at: Optional[int] = None

    def mute(self, now: int) -> None:
        self._muted_until = now + self.thresholds.reminder_interval_seconds

    def delay(self, now: int, seconds: int = 30 * 60) -> None:
        if seconds <= 0:
            raise ValueError("delay seconds must be positive")
        self._delayed_until = now + seconds

    def process(self, data: SensorData) -> AirStatus:
        if self._started_at is None:
            self._started_at = data.timestamp

        filtered = self._filter(data)
        now = data.timestamp

        if now - self._started_at < self.warmup_seconds:
            self._state = AirState.WARMUP
            self._reset_counters()
        elif not self._valid_for_decision(filtered):
            self._state = AirState.SENSOR_ERROR
            self._reset_counters()
        else:
            self._update_state(filtered, now)

        buzzer = self._should_buzz(now)
        if buzzer:
            self._last_reminder_at = now

        return AirStatus(
            state=self._state,
            reminder=self._reminder_level(),
            message=self._message(),
            led=self._led(),
            buzzer=buzzer,
            muted=now < self._muted_until,
            delayed=now < self._delayed_until,
            filtered=filtered,
        )

    def _filter(self, data: SensorData) -> SensorData:
        co2 = self._co2_filter.update(data.co2) if data.co2_valid and data.co2 is not None else None
        temperature = (
            self._temp_filter.update(data.temperature)
            if data.temperature_valid and data.temperature is not None
            else None
        )
        humidity = (
            self._humidity_filter.update(data.humidity)
            if data.humidity_valid and data.humidity is not None
            else None
        )
        pm25 = self._pm25_filter.update(data.pm25) if data.pm25_valid and data.pm25 is not None else None
        return SensorData(
            timestamp=data.timestamp,
            co2=co2,
            temperature=temperature,
            humidity=humidity,
            pm25=pm25,
            co2_valid=co2 is not None,
            temperature_valid=temperature is not None,
            humidity_valid=humidity is not None,
            pm25_valid=pm25 is not None,
        )

    def _valid_for_decision(self, data: SensorData) -> bool:
        return (
            data.co2_valid
            and data.temperature_valid
            and data.humidity_valid
            and 300 <= data.co2 <= 10000
            and -10 <= data.temperature <= 60
            and 0 <= data.humidity <= 100
        )

    def _update_state(self, data: SensorData, now: int) -> None:
        co2 = data.co2
        assert co2 is not None

        if co2 >= self.thresholds.co2_high:
            self._over_limit_count += 1
            self._normal_count = 0
            if self._over_limit_count >= self.thresholds.required_over_limit_samples:
                self._state = AirState.HIGH
            else:
                self._state = AirState.ATTENTION
            return

        if co2 >= self.thresholds.co2_attention:
            self._over_limit_count += 1
            self._normal_count = 0
            if self._over_limit_count >= self.thresholds.required_over_limit_samples:
                self._state = AirState.VENTILATE
            else:
                self._state = AirState.ATTENTION
            return

        if co2 <= self.thresholds.co2_recover:
            self._normal_count += 1
            if self._state in (AirState.VENTILATE, AirState.HIGH):
                if self._recovering_since is None:
                    self._recovering_since = now
                self._state = AirState.RECOVERING
                if now - self._recovering_since >= self.thresholds.recovery_hold_seconds:
                    self._state = AirState.NORMAL
                    self._reset_counters()
            elif self._state == AirState.WARMUP:
                self._state = AirState.NORMAL
                self._reset_counters()
            elif self._state in (AirState.ATTENTION, AirState.RECOVERING):
                if self._normal_count >= self.thresholds.required_normal_samples:
                    self._state = AirState.NORMAL
                    self._reset_counters()
            else:
                self._state = AirState.NORMAL
            return

        self._normal_count = 0
        self._recovering_since = None
        if self._state == AirState.WARMUP:
            self._state = AirState.NORMAL

    def _reset_counters(self) -> None:
        self._over_limit_count = 0
        self._normal_count = 0
        self._recovering_since = None

    def _should_buzz(self, now: int) -> bool:
        if self._state not in (AirState.VENTILATE, AirState.HIGH, AirState.SENSOR_ERROR):
            return False
        if now < self._muted_until or now < self._delayed_until:
            return False
        if self._last_reminder_at is not None:
            if now - self._last_reminder_at < self.thresholds.reminder_interval_seconds:
                return False
        return True

    def _reminder_level(self) -> ReminderLevel:
        return {
            AirState.WARMUP: ReminderLevel.NONE,
            AirState.NORMAL: ReminderLevel.NONE,
            AirState.ATTENTION: ReminderLevel.LOW,
            AirState.RECOVERING: ReminderLevel.LOW,
            AirState.VENTILATE: ReminderLevel.MEDIUM,
            AirState.HIGH: ReminderLevel.HIGH,
            AirState.SENSOR_ERROR: ReminderLevel.HIGH,
        }[self._state]

    def _message(self) -> str:
        return {
            AirState.WARMUP: "设备预热中，暂不提醒",
            AirState.NORMAL: "空气状态正常",
            AirState.ATTENTION: "CO₂正在升高，可稍后短时通风",
            AirState.VENTILATE: "房间可能有些闷，建议开窗通风5-10分钟",
            AirState.HIGH: "CO₂持续偏高，建议尽快检查通风情况",
            AirState.RECOVERING: "正在通风，空气状况正在改善",
            AirState.SENSOR_ERROR: "传感器读数异常，请检查连接和进风口",
        }[self._state]

    def _led(self) -> str:
        return {
            AirState.WARMUP: "WHITE",
            AirState.NORMAL: "GREEN",
            AirState.ATTENTION: "YELLOW",
            AirState.VENTILATE: "ORANGE",
            AirState.HIGH: "RED",
            AirState.RECOVERING: "BLUE",
            AirState.SENSOR_ERROR: "WHITE_FLASH",
        }[self._state]


class CsvLogger:
    def __init__(self, target: str | Path | TextIO) -> None:
        self._owns_file = not hasattr(target, "write")
        self._file = open(target, "w", newline="", encoding="utf-8") if self._owns_file else target
        self._writer = csv.writer(self._file)
        self._writer.writerow(["timestamp", "co2", "temperature", "humidity", "pm25", "state", "message"])

    def write(self, status: AirStatus) -> None:
        data = status.filtered
        self._writer.writerow(
            [
                data.timestamp,
                f"{data.co2:.1f}" if data.co2 is not None else "",
                f"{data.temperature:.1f}" if data.temperature is not None else "",
                f"{data.humidity:.1f}" if data.humidity is not None else "",
                f"{data.pm25:.1f}" if data.pm25 is not None else "",
                status.state.value,
                status.message,
            ]
        )
        self._file.flush()

    def close(self) -> None:
        if self._owns_file:
            self._file.close()

    def __enter__(self) -> "CsvLogger":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
