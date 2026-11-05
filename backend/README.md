# FastAPI + MQTT + SQLite后端

## 启动

```powershell
py -m pip install -r .\backend\requirements.txt
$env:AIR_DB_PATH = "$PWD\backend\air_monitor.db"
$env:MQTT_HOST = "127.0.0.1"
py -m uvicorn backend.app:app --host 0.0.0.0 --port 8000
```

不设置`MQTT_HOST`时，仍可使用REST接口进行本地开发和模拟测试。
启动后访问`http://127.0.0.1:8000/`即可打开监控页面，不需要直接双击HTML文件。

## 接口

- `GET /api/health`
- `POST /api/readings`
- `GET /api/devices`
- `GET /api/latest?device_id=...`
- `GET /api/history?device_id=...&metric=aq_index`
- `GET /api/alarms`
- `POST /api/alarms/{id}/ack`
- `PUT /api/threshold`

MQTT订阅主题：`home/+/+/data`，QoS 1。

默认告警分级与方案一致：温度26/29/30°C、湿度60/70/75%RH、空气指数1.0/1.5/2.5。实际使用时应通过`PUT /api/threshold`按房间基线调整。
