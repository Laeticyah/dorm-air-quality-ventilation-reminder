# ESP32固件

对应实施方案中的ESP32 DevKitC、DHT22、MQ-135和MQTT链路。

## GPIO约定

- DHT22 DATA：GPIO4
- MQ-135 AO：GPIO34（ADC）
- 状态LED：GPIO25
- DHT22建议使用4.7k上拉
- MQ-135使用5V供电，ADC输入必须确认电平安全

## 配置与烧录

修改`src/main.cpp`中的WiFi、MQTT服务器、设备ID和房间标识，然后使用PlatformIO编译烧录。

固件启动后通过NTP校时，发布：

`home/{room}/{device}/data`

状态遗嘱：

`home/{room}/{device}/status`

MQTT消息包含`device_id`、`room`、`ts`、`temp`、`humi`、`aq_raw`、`aq_index`、`rssi`和`flag`字段。

MQ-135的`aq_index`当前是原始ADC归一化值，不代表真实CO₂ ppm；正式项目中应在报告中明确这一点，并在第6周替换或补充NDIR CO₂传感器。
