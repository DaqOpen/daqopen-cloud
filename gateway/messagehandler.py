import sqlite3
import json
import gzip
import datetime
from pathlib import Path
from paho.mqtt import client as mqtt
from influxdb import InfluxDBClient
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app_env = os.getenv("DAQOPEN_ENV", "development")
if app_env == "development":
    from dotenv import load_dotenv
    load_dotenv()

CONFIG_DB_PATH = Path(os.getenv("DAQOPEN_CONFIG_DB_PATH", "../devices.sq3"))
INFLUXDB_HOST = os.getenv("DAQOPEN_INFLUXDB_HOST", "localhost")
MQTT_HOST = os.getenv("DAQOPEN_MQTT_HOST","localhost")

@dataclass
class DeviceInfo:
    device_id: str
    friendly_name: str = "box01"
    location: str = "unkown"
    target_database: str = "daqopen"
    aggregated_data_measurement: str = "agg_data"
    daqinfo: Dict[str, Any] = field(default_factory=dict)
    last_seen: datetime.datetime = None


def read_device_info(db_path: Path, device_id: str):
    # SQLite-Abfrage
    conn = sqlite3.connect(db_path.as_posix())
    cursor = conn.cursor()
    cursor.execute("SELECT device_info FROM devices WHERE device_id=?", (device_id,))
    return_value = cursor.fetchone()
    if return_value:
        info = json.loads(return_value[0])
        info["device_id"] = device_id
        device_info = DeviceInfo(**info) # Convert to DeviceInfo object
        return device_info
    else:
        return None

def decode_payload(payload: bytes, encoding: str):
    if encoding == "gjson":
        payload_dict = json.loads(gzip.decompress(payload))
    elif encoding == "json":
        payload_dict = json.loads(payload)

    return payload_dict

def handle_message(client, userdata, msg):
    logger.debug("New Message")
    parts = msg.topic.split("/")
    # Topic signature must be of type: */*/{device_id}/{data_type}/{encoding}
    if len(parts) != 5:
        return
    
    device_id, data_type, encoding = parts[2], parts[3], parts[4]
    device_info = read_device_info(CONFIG_DB_PATH, device_id)

    if not device_info:
        logger.warning(f"Device ID {device_id} not in database")
        return None
    
    # Database insertion
    with InfluxDBClient(host=INFLUXDB_HOST, database=device_info["target_database"]) as db_client:
        data = decode_payload(msg.payload, encoding)
        if data_type == "agg_data":
            scalar_data = {key: value for key, value in data["data"].items() if isinstance(value, (float, int))}
            db_dict = {
                "measurement": "agg_data",
                "tags": {
                    "device_id": device_info.device_id,
                    "location": device_info.location
                },
                "time": datetime.datetime.fromtimestamp(data["timestamp"], tz=datetime.UTC),
                "fields": scalar_data
            }
        if db_dict:
            db_client.write_points([db_dict])

if __name__ == "__main__":
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, clean_session=False)
    client.on_message = handle_message
    client.connect("localhost")
    client.subscribe("dt/pqopen/#", qos=2)
    client.loop_forever()
