# 宿舍空气质量与通风提醒：9周方案程序原型

当前版本按《室内环境监测系统_9周实施方案》拆成三层，先不依赖硬件即可验证业务逻辑：

- `SensorData`：统一真实传感器和模拟数据接口；
- `AirMonitor`：滤波、连续超限、回差、恢复观察和提醒去抖；
- `CsvLogger`：输出可用Excel分析的CSV日志；
- `run_simulation.py`：运行正常、CO₂升高、开窗恢复、颗粒物尖峰和传感器异常场景；
- `tests/test_air_monitor.py`：自动化验证核心状态机。
- `backend/`：FastAPI + SQLite WAL + MQTT订阅后端；
- `firmware/`：PlatformIO ESP32 + DHT22 + MQ-135 + MQTT固件骨架；
- `web/`：无构建依赖的基础监控页面。

## 运行

Windows下使用Python启动模拟器：

```powershell
py .\run_simulation.py --scenario rising
py .\run_simulation.py --scenario window --csv .\data\window.csv
py .\run_simulation.py --scenario dust
py .\run_simulation.py --scenario sensor-error
```

运行测试：

```powershell
py -m unittest discover -s tests -v
```

## 启动后端

```powershell
py -m pip install -r .\backend\requirements.txt
py -m uvicorn backend.app:app --host 0.0.0.0 --port 8000
```

启动后浏览器访问`http://127.0.0.1:8000/`即可查看监控页面；页面支持设备卡片、趋势图、告警确认和在线状态。

## 下一步接入硬件

真实传感器只需要实现同样的`SensorData`输出，不需要改动`AirMonitor`。根据9周计划，建议先完成W1-W4的本地闭环和MQTT链路，再在W5加入告警确认，在W6-W8完成网页、72小时稳定性和性能测试。
