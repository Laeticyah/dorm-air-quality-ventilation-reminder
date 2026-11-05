from __future__ import annotations

import argparse
import sys
from pathlib import Path

from src.air_monitor import AirMonitor, AirState, CsvLogger, SensorData

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


def sample(timestamp: int, co2: float, humidity: float = 60, pm25: float = 12) -> SensorData:
    return SensorData(
        timestamp=timestamp,
        co2=co2,
        temperature=24.0,
        humidity=humidity,
        pm25=pm25,
    )


def build_scenario(name: str) -> list[SensorData]:
    if name == "rising":
        values = [700, 780, 860, 940, 1020, 1100, 1200, 1450, 1650, 1850]
    elif name == "window":
        values = [1200, 1300, 1450, 1300, 1100, 900, 820, 760, 720]
    elif name == "dust":
        return [sample(i * 30, 760, pm25=120 if 3 <= i <= 5 else 12) for i in range(10)]
    elif name == "sensor-error":
        return [
            SensorData(i * 30, None, 24.0, 60.0, co2_valid=False)
            for i in range(10)
        ]
    else:
        values = [720] * 10
    return [sample(i * 30, value) for i, value in enumerate(values)]


def run(name: str, output: Path | None) -> None:
    monitor = AirMonitor(warmup_seconds=0)
    logger = CsvLogger(output) if output else None
    try:
        previous = None
        for data in build_scenario(name):
            status = monitor.process(data)
            changed = previous != status.state
            marker = "*" if changed else " "
            print(
                f"{marker} t={data.timestamp:>4}s "
                f"CO2={status.filtered.co2!s:>6} "
                f"state={status.state.value:<11} "
                f"LED={status.led:<11} "
                f"buzz={'yes' if status.buzzer else 'no ':<3} "
                f"{status.message}"
            )
            if logger:
                logger.write(status)
            previous = status.state
    finally:
        if logger:
            logger.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="宿舍空气质量提醒软件模拟器")
    parser.add_argument(
        "--scenario",
        choices=["normal", "rising", "window", "dust", "sensor-error"],
        default="rising",
    )
    parser.add_argument("--csv", type=Path, help="可选：输出CSV日志文件")
    args = parser.parse_args()
    run(args.scenario, args.csv)
