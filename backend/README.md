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
生产或局域网部署时，请参考根目录`.env.example`配置MQTT用户名、密码和TLS；不要把真实`.env`提交到仓库。

## 接口

- `GET /api/health`
- `POST /api/maintenance/cleanup`：按保留周期清理原始读数和历史告警
- `POST /api/maintenance/backup`：创建SQLite数据库备份
- `POST /api/readings`
- `GET /api/devices`
- `POST /api/devices`：手动注册设备
- `GET/PUT /api/devices/{device_id}`：查看和维护设备名称、房间信息
- `DELETE /api/devices/{device_id}`：删除设备；有历史数据时需显式传`delete_readings=true`
- `GET /api/latest?device_id=...`
- `GET /api/history?device_id=...&metric=aq_index`（也支持`co2_ppm`）
- `GET /api/readings/page`：按设备和时间范围分页读取原始数据
- `GET /api/summary`
- `GET /api/statistics`：查询单项指标的数量、平均值、最大值和最小值
- `GET /api/statistics/alarms`：查询告警总数、确认数、恢复数和级别分布
- `GET /api/export.csv?device_id=...`
- `GET /api/alarms`
- `GET /api/alarms/page`：按确认状态、设备、时间范围分页查询告警
- `POST /api/alarms/{id}/ack`
- `PUT /api/threshold`
- `GET/POST /api/ventilation-events`
- `GET /api/ventilation-events/{event_id}/analysis`：分析通风前后CO₂变化

MQTT订阅主题：`home/+/+/data`，QoS 1。`/api/health`中的`mqtt`表示当前已连接，`mqtt_configured`表示是否配置了MQTT主机。

默认告警分级与方案一致：温度26/29/30°C、湿度60/70/75%RH、空气指数1.0/1.5/2.5、CO₂ 1000/1400/1800ppm。恢复条件默认使用对应指标的`warn_hi`，实际使用时应通过`PUT /api/threshold`按房间基线调整。

超限和恢复均要求连续3次有效采样，避免单点噪声产生告警。设备超过90秒未上报时，在设备查询过程中生成设备离线告警；设备重新上线后自动恢复该告警。

读数接口兼容后续真实CO₂传感器字段：`co2_ppm`、`co2_valid`和`sensor_status`。当前ESP32固件仍可只上报原有字段，硬件升级后无需修改数据库和前端接口。

数据清理由后台线程按`CLEANUP_INTERVAL_SECONDS`执行，默认原始数据保留30天，可通过`RAW_DATA_RETENTION_DAYS`和`LOG_LEVEL`环境变量配置。

数据库备份默认写入`backups`目录，可通过`BACKUP_DIR`环境变量修改。通风效果以通风前后CO₂下降至少100ppm作为有效的演示判据，实际验收时应结合现场采样间隔校准。
