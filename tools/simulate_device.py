"""Send deterministic sensor scenarios to the local monitoring API."""

from __future__ import annotations

import argparse
import json
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def build_reading(device_id: str, room: str, timestamp: int, step: int, scenario: str) -> dict:
    values = {
        "normal": (23.5, 52.0, 620.0, 0.65),
        "rising": (24.0 + step * 0.15, 55.0, 720.0 + step * 85.0, 0.7),
        "window": (25.0, 65.0, max(620.0, 1900.0 - step * 180.0), 0.8),
        "spike": (24.0, 55.0, 780.0, 0.9 if 3 <= step <= 4 else 0.7),
        "error": (None, None, None, None),
    }
    temp, humi, co2, aq_index = values[scenario]
    return {
        "device_id": device_id,
        "room": room,
        "ts": timestamp,
        "temp": temp,
        "humi": humi,
        "co2_ppm": co2,
        "co2_valid": scenario != "error",
        "sensor_status": "ERROR" if scenario == "error" else "OK",
        "aq_index": aq_index,
        "aq_raw": int(aq_index * 1000) if aq_index is not None else None,
        "rssi": -55,
        "flag": "SENSOR_ERROR" if scenario == "error" else "OK",
    }


def post_json(endpoint: str, payload: dict) -> None:
    body = json.dumps(payload).encode("utf-8")
    request = Request(
        endpoint,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=5) as response:
            response.read()
    except (HTTPError, URLError) as exc:
        raise RuntimeError(f"cannot post reading to {endpoint}: {exc}") from exc


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", choices=("normal", "rising", "window", "spike", "error"), default="rising")
    parser.add_argument("--device-id", default="esp32-sim")
    parser.add_argument("--room", default="dorm-302")
    parser.add_argument("--endpoint", default="http://127.0.0.1:8000/api/readings")
    parser.add_argument("--count", type=int, default=12)
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--no-wait", action="store_true")
    args = parser.parse_args()
    if args.count < 1:
        parser.error("--count must be positive")
    for step in range(args.count):
        payload = build_reading(args.device_id, args.room, int(time.time()), step, args.scenario)
        post_json(args.endpoint, payload)
        print(
            f"{args.device_id}: scenario={args.scenario} "
            f"co2={payload['co2_ppm']} aq={payload['aq_index']}"
        )
        if not args.no_wait and step + 1 < args.count:
            time.sleep(max(args.interval, 0.0))


if __name__ == "__main__":
    main()
