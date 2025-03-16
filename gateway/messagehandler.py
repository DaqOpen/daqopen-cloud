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
import math
from copy import deepcopy
import time

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app_env = os.getenv("DAQOPEN_ENV", "development")
if app_env == "development":
    from dotenv import load_dotenv
    load_dotenv()

CONFIG_DB_PATH = Path(os.getenv("DAQOPEN_CONFIG_DB_PATH", "../devices.sq3"))
INFLUXDB_HOST = os.getenv("DAQOPEN_INFLUXDB_HOST", "localhost")
MQTT_HOST = os.getenv("DAQOPEN_MQTT_HOST","localhost")
MQTT_PORT = os.getenv("DAQOPEN_MQTT_PORT",1883)
CACHE_PATH = os.getenv("DAQOPEN_CACHE_PATH","../data_cache.sq3")

@dataclass
class DeviceInfo:
    device_id: str
    friendly_name: str = "box01"
    location: str = "unkown"
    target_database: str = "daqopen"
    aggregated_data_measurement: str = "agg_data"
    daqinfo: Dict[str, Any] = field(default_factory=dict)
    last_seen: datetime.datetime = None


def aggregated_data_to_json_list(data: dict, device_info: DeviceInfo):
    db_dict_template = {
        "measurement": "agg_data",
        "tags": {
            "device_id": device_info.device_id,
            "location": device_info.location,
            "interval_sec": data["interval_sec"]
        },
        "time": datetime.datetime.fromtimestamp(data["timestamp"], tz=datetime.UTC),
        "fields": {}
    }
    db_scalar_dict = deepcopy(db_dict_template)
    db_list = []
    db_vector_dict = {}
    for ch_name, val in data['data'].items():
        if type(val) in [list]:
            for idx, item in enumerate(val):
                if math.isnan(item):
                    continue
                if idx not in db_vector_dict:
                    db_vector_dict[idx] = deepcopy(db_dict_template)
                    db_vector_dict[idx]["tags"]["order"] = f"{idx:02d}"
                db_vector_dict[idx]["fields"][ch_name] = item
        else:
            if val is None:
                continue
            db_scalar_dict['fields'][ch_name] = val
    db_list.append(db_scalar_dict)
    db_list.extend(db_vector_dict.values())
    return db_list

def dataseries_to_json_list(data: dict, device_info: DeviceInfo):
    db_list = []
    tags = {'device_id': device_info.device_id,
            'location': device_info.location}
    # Iterate thru data Channels
    for ch_name, ch_data in data['data'].items():
        for idx, ts in enumerate(ch_data['timestamps']):
            json_tmp = {'measurement': "dataseries",
                        'tags': tags,
                        'time': ts,
                        'fields': {ch_name: ch_data['data'][idx]}}
            db_list.append(json_tmp.copy())
    return db_list

def event_to_json_list(data: dict, device_info: DeviceInfo):
    json_tmp = {
        "measurement": "events",
        "tags": {
            'device_id': device_info.device_id,
            'location': device_info.location,
            'event_type': data["event_type"],
            'channel': data["channel"]
        },
        "time": datetime.datetime.fromtimestamp(data["timestamp"], tz=datetime.UTC),
        "fields": data["data"]
    }
    return [json_tmp]

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

def cache_data(data_type: str, device_info: DeviceInfo, data: dict):
    conn = sqlite3.connect(CACHE_PATH)
    conn.execute("CREATE TABLE IF NOT EXISTS data_cache (id INTEGER PRIMARY KEY, data_type TEXT, device_info TEXT, data TEXT);")
    with conn:
        conn.execute("INSERT INTO data_cache (data_type, device_info, data) VALUES (?, ?, ?);", (data_type, json.dumps(device_info.__dict__), json.dumps(data)))

def get_cached_data() -> tuple | None:
    conn = sqlite3.connect(CACHE_PATH)
    with conn:
        res = conn.execute("SELECT * FROM data_cache LIMIT 1;")
        row = res.fetchone()
        if row:
            id = row[0]
            data_type = row[1]
            device_info = DeviceInfo(**json.loads(row[2]))
            data = json.loads(row[3])
            return id, data_type, device_info, data
        else:
            return None
    
def delete_cached_data(id: int):
    conn = sqlite3.connect(CACHE_PATH)
    with conn:
        res = conn.execute("DELETE FROM data_cache WHERE id = ?;", (id,))

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
    data = decode_payload(msg.payload, encoding)
    res = insert_data_database(data_type, device_info, data)
    if not res:
        cache_data(data_type, device_info, data)


def insert_data_database(data_type: str, device_info: DeviceInfo, data: dict):
    with InfluxDBClient(host=INFLUXDB_HOST) as db_client:
        try:
            target_database = device_info.target_database
            if data_type == "agg_data":
                db_client.write_points(aggregated_data_to_json_list(data, device_info), database=target_database)
            if data_type == "dataseries":
                db_client.write_points(dataseries_to_json_list(data, device_info), database=target_database, time_precision='u')
            if data_type == "event":
                db_client.write_points(event_to_json_list(data, device_info), database=target_database, time_precision='u')
            result = True
        except Exception as e:
            logger.error(getattr(e, 'message', repr(e)))
            result = False
    return result
            

if __name__ == "__main__":
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="daqopen-gateway", clean_session=False)
    client.on_message = handle_message
    client.connect_async(MQTT_HOST, MQTT_PORT)
    client.subscribe("pqopen/dt/#", qos=2)
    client.loop_start()
    while True:
        cached_data = get_cached_data()
        if cached_data:
            res = insert_data_database(cached_data[1], cached_data[2], cached_data[3])
            logger.debug("Insert cached data to database...")
            if res:
                delete_cached_data(cached_data[0])
                logger.debug(f"Insert was successful - delete cache with id={cached_data[0]:d}")
        else:
            time.sleep(1)


