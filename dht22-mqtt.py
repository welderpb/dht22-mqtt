import logging
import os
import time
from pathlib import Path

import adafruit_dht
import paho.mqtt.publish as publish
import signal

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(name)s] %(levelname)8s %(message)s')


# Config from environment (see Dockerfile)
DHT22_PIN = int(os.getenv('DHT22_PIN', '4'))
DHT22_CHECK_EVERY = int(os.getenv('DHT22_CHECK_EVERY', 1))
MQTT_SERVICE_HOST = os.getenv('MQTT_SERVICE_HOST', 'mosquitto.local')
MQTT_SERVICE_PORT = int(os.getenv('MQTT_SERVICE_PORT', 1883))
MQTT_SERVICE_USER = os.getenv('MQTT_SERVICE_USER', None)
MQTT_SERVICE_PASSWORD = os.getenv('MQTT_SERVICE_PASSWORD', None)
MQTT_SERVICE_TOPIC = os.getenv('MQTT_SERVICE_TOPIC', 'home/livingroom')
MQTT_CLIENT_ID = os.getenv('MQTT_CLIENT_ID', os.getenv('HOSTNAME'))
HA_NAME = os.getenv('HA_NAME', None)

logger = logging.getLogger(MQTT_CLIENT_ID)

class GracefulKiller:
    def __init__(self):
        self.kill_now = False
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        logging.warning('gracefully exitting')
        self.kill_now = True


if __name__ == "__main__":
    g = GracefulKiller()

    # Display config on startup
    logger.debug("#" * 80)
    logger.debug(f"# {DHT22_PIN=}")
    logger.debug(f"# {DHT22_CHECK_EVERY=}")
    logger.debug(f"# {MQTT_SERVICE_HOST=}")
    logger.debug(f"# {MQTT_SERVICE_PORT=}")
    logger.debug(f"# {MQTT_SERVICE_USER=}")
    logger.debug(f"# {MQTT_SERVICE_PASSWORD=}")
    logger.debug(f"# {MQTT_SERVICE_TOPIC=}")
    logger.debug(f"# {MQTT_CLIENT_ID=}")
    logger.debug("#" * 80)

    MQTT_SERVICE_AUTH = None

    if MQTT_SERVICE_USER is not None:
        MQTT_SERVICE_AUTH = {'username':MQTT_SERVICE_USER, 'password':MQTT_SERVICE_PASSWORD}

    if HA_NAME is not None:
        temp_config = '''{
          "state_topic": "DHT22/%(HA_NAME)s/temp",
          "icon": "hass:thermometer",
          "name": "%(HA_NAME)s Temperature",
          "unique_id": "dht22_%(HA_NAME)s_temp",
          "unit_of_measurement": "°C",
          "state_class": "measurement", 
          "device": {
             "identifiers": ["%(HA_NAME)s"],
             "manufacturer": "Unknown",
             "model": "DHT22",
             "name": "%(HA_NAME)s"
          }
        }'''
        hum_config = '''{
          "state_topic": "DHT22/%(HA_NAME)s/hum",
          "icon": "mdi:air-humidifier",
          "name": "%(HA_NAME)s Humidity",
          "unique_id": "dht22_%(HA_NAME)s_hum",
          "unit_of_measurement": "%%",
          "state_class": "measurement", 
          "device": {
             "identifiers": ["%(HA_NAME)s"],
             "manufacturer": "Unknown",
             "model": "DHT22",
             "name": "%(HA_NAME)s"
          }
        }'''

        # Prepare sensors config to be published on MQTT
        cfgs = [(f"homeassistant/sensor/DHT22/{HA_NAME}_temp/config", temp_config % {"HA_NAME": HA_NAME}),
                (f"homeassistant/sensor/DHT22/{HA_NAME}_hum/config", hum_config % {"HA_NAME": HA_NAME})]
        MQTT_SERVICE_TOPIC = f"DHT22/{HA_NAME}"

    # Initializes DHT22 on given GPIO pin
    dht22_sensor = adafruit_dht.DHT22(DHT22_PIN)

    while not g.kill_now:

        try:
            # Read from sensor
            temperature = dht22_sensor.temperature
            humidity = dht22_sensor.humidity
            # Touch file every time data is read (used for liveness probe in k8s)
            Path('.dht22_updated').touch()
        except RuntimeError as e:
            logger.error(str(e))
            time.sleep(5)
            continue

        logger.info(f"[{MQTT_SERVICE_TOPIC}/temperature] --- {temperature}°C ---> [{MQTT_SERVICE_HOST}:{MQTT_SERVICE_PORT}]")
        logger.info(f"[{MQTT_SERVICE_TOPIC}/humidity] ------ {humidity}% ----> [{MQTT_SERVICE_HOST}:{MQTT_SERVICE_PORT}]")

        if humidity > 101:
            # skip unreal values
            continue

        try:
            # Prepare messages to be published on MQTT
            msgs = [(f"{MQTT_SERVICE_TOPIC}/temp", str(temperature)),
                    (f"{MQTT_SERVICE_TOPIC}/hum", str(humidity))]

            # Publish messages on given MQTT broker
            logger.info("Sending sensor config.")
            publish.multiple(cfgs, hostname=MQTT_SERVICE_HOST, port=MQTT_SERVICE_PORT, client_id=MQTT_CLIENT_ID, auth=MQTT_SERVICE_AUTH)
            logger.info("Sending sensor data.")
            publish.multiple(msgs, hostname=MQTT_SERVICE_HOST, port=MQTT_SERVICE_PORT, client_id=MQTT_CLIENT_ID, auth=MQTT_SERVICE_AUTH)
        except Exception:
            logger.error("An error occured publishing values to MQTT", exc_info=True)

        # Sleep a little
        time.sleep(DHT22_CHECK_EVERY)
