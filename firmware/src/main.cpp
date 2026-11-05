#include <Arduino.h>
#include <ArduinoJson.h>
#include <DHT.h>
#include <PubSubClient.h>
#include <WiFi.h>
#include <time.h>

// 方案中的ESP32 DevKitC / DHT22 / MQ-135 / BH1750接口约定。
// BH1750在后续固件迭代接入；当前MQTT数据先保留lux字段。
constexpr uint8_t DHT_PIN = 4;
constexpr uint8_t DHT_TYPE = DHT22;
constexpr uint8_t MQ135_PIN = 34;
constexpr uint8_t LED_PIN = 25;

const char *WIFI_SSID = "YOUR_WIFI_SSID";
const char *WIFI_PASSWORD = "YOUR_WIFI_PASSWORD";
const char *MQTT_HOST = "192.168.1.10";
constexpr uint16_t MQTT_PORT = 1883;
const char *DEVICE_ID = "esp32-a1";
const char *ROOM = "dorm-302";

DHT dht(DHT_PIN, DHT_TYPE);
WiFiClient wifiClient;
PubSubClient mqtt(wifiClient);
unsigned long lastPublish = 0;
unsigned long lastWifiAttempt = 0;
unsigned long lastMqttAttempt = 0;
constexpr unsigned long PUBLISH_INTERVAL_MS = 30000;
constexpr unsigned long RECONNECT_INTERVAL_MS = 5000;

void connectWifi() {
  if (WiFi.status() == WL_CONNECTED) return;
  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
}

void connectMqtt() {
  if (mqtt.connected() || WiFi.status() != WL_CONNECTED) return;
  String statusTopic = "home/" + String(ROOM) + "/" + String(DEVICE_ID) + "/status";
  String clientId = String(DEVICE_ID) + "-" + String((uint32_t)ESP.getEfuseMac(), HEX);
  if (mqtt.connect(clientId.c_str(), nullptr, nullptr, statusTopic.c_str(), 1, true, "offline")) {
    mqtt.publish(statusTopic.c_str(), "online", true);
  }
}

void publishReading() {
  const float temperature = dht.readTemperature();
  const float humidity = dht.readHumidity();
  const int aqRaw = analogRead(MQ135_PIN);
  const bool valid = !isnan(temperature) && !isnan(humidity);

  JsonDocument doc;
  doc["device_id"] = DEVICE_ID;
  doc["room"] = ROOM;
  doc["ts"] = (uint32_t)time(nullptr);
  doc["temp"] = valid ? temperature : 0;
  doc["humi"] = valid ? humidity : 0;
  doc["aq_raw"] = aqRaw;
  doc["aq_index"] = aqRaw / 1000.0; // 仅为方案中的趋势指数，不是CO₂ ppm
  doc["rssi"] = WiFi.RSSI();
  doc["flag"] = valid ? "OK" : "SENSOR_ERR";

  char payload[384];
  serializeJson(doc, payload, sizeof(payload));
  const String topic = "home/" + String(ROOM) + "/" + String(DEVICE_ID) + "/data";
  mqtt.publish(topic.c_str(), payload, false);
  digitalWrite(LED_PIN, valid ? HIGH : LOW);
}

void setup() {
  Serial.begin(115200);
  pinMode(LED_PIN, OUTPUT);
  dht.begin();
  connectWifi();
  configTime(8 * 3600, 0, "pool.ntp.org", "ntp.aliyun.com");
  mqtt.setServer(MQTT_HOST, MQTT_PORT);
}

void loop() {
  const unsigned long now = millis();
  if (WiFi.status() != WL_CONNECTED && now - lastWifiAttempt >= RECONNECT_INTERVAL_MS) {
    lastWifiAttempt = now;
    connectWifi();
  }
  if (!mqtt.connected() && now - lastMqttAttempt >= RECONNECT_INTERVAL_MS) {
    lastMqttAttempt = now;
    connectMqtt();
  }
  mqtt.loop();

  if (mqtt.connected() && now - lastPublish >= PUBLISH_INTERVAL_MS) {
    lastPublish = now;
    publishReading();
  }
}
