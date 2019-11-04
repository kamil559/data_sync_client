import os
import uuid
from dotenv import load_dotenv
from ftplib import FTP

import redis
import requests
import websocket

from utils import get_json, parse_message_data

try:
    import thread
except ImportError:
    import _thread as thread

load_dotenv()


class WebsocketConnection:
    CACHE_IDS = []
    FTP_HOST = os.getenv("FTP_HOST")
    FTP_USERNAME = os.getenv("FTP_USERNAME")
    FTP_PASSWORD = os.getenv("FTP_PASSWORD")

    def __init__(self, cycle_amount: int, interval: int, socket_id: uuid, socket_name: str,
                 debug: bool = False, *args, **kwargs) -> None:
        websocket.enableTrace(debug)
        self.cycle_amount = cycle_amount
        self.interval = interval
        self.socket_id = socket_id
        self.socket_name = socket_name

        self.connect()

    def connect(self):
        ws = websocket.WebSocketApp(
            f"ws://127.0.0.1:8000/measurements/{self.cycle_amount}/{self.interval}/{self.socket_id}/{self.socket_name}",
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )

        ws.on_open = self.on_open
        ws.run_forever()

    @staticmethod
    def on_open(ws):
        print("websocket has been opened!")

    @staticmethod
    def on_message(ws, message_data):
        message_data = parse_message_data(message_data)

        file_identifier = message_data.get("file_identifier")
        timestamp_data = message_data.get("timestamp_data")

        redis_cache.set(file_identifier, timestamp_data)
        WebsocketConnection.CACHE_IDS.append(file_identifier)

        print(timestamp_data)

    @staticmethod
    def on_error(ws, error):
        print(error)

    @classmethod
    def on_close(cls):
        FTPConnection(cls.FTP_HOST, cls.FTP_USERNAME, cls.FTP_PASSWORD)
        print("websocket has been closed!")


def health_check() -> int:
    response = requests.get("http://127.0.0.1:8000/api/health-check")
    assert response.status_code == 200
    return response.status_code


def get_server_time(unique_id: uuid) -> dict:
    response = requests.get(f"http://127.0.0.1:8000/api/timestamp-sync/{unique_id}")
    assert response.status_code == 200
    return get_json(response.content)


class FTPConnection:

    def __init__(self, host: str, username: str, password: str) -> None:
        self.host = host
        self.username = username
        self.password = password
        self.ftp_connection = None

        self.connect()
        self.save_data(WebsocketConnection.CACHE_IDS)
        self.ftp_connection.quit()

        self.compare_data(WebsocketConnection.CACHE_IDS)

        self.clear_cache_keys(WebsocketConnection.CACHE_IDS)

    def connect(self):
        self.ftp_connection = FTP(self.host)
        self.ftp_connection.login(user=self.username, passwd=self.password)

    def save_data(self, cache_ids: list):
        for cache_id in cache_ids:
            redis_cache.get(cache_id)

            with open(f"downloads/{cache_id}.txt", 'wb') as file:
                self.ftp_connection.retrbinary(f'RETR {cache_id}.txt', file.write)

    @staticmethod
    def compare_data(cache_ids: list):
        for cache_id in cache_ids:
            with open(f"downloads/{cache_id}.txt", 'r') as file:
                file_data = file.read()
                if file_data != redis_cache.get(cache_id).decode():
                    print(file_data)

    @staticmethod
    def clear_cache_keys(cache_ids: list):
        for cache_id in cache_ids:
            redis_cache.delete(cache_id)

        WebsocketConnection.CACHE_IDS = []


class Client:

    def __init__(self, *args, **kwargs):
        self.cycle_amount = os.getenv("MEASUREMENT_CYCLES", 5)
        self.interval = os.getenv("MEASUREMENT_INTERVAL", 5000)
        self.room_name = os.getenv("SOCKET_ROOM_NAME")

        self.socket_id = uuid.uuid4()
        self.sync_timestamp = None

    def health_check_status_code(self):
        health_check()

    def sync_time(self):
        server_time = get_server_time(unique_id=self.socket_id)
        self.sync_timestamp = server_time.get("timestamp")

    def measure_timestamps(self):
        WebsocketConnection(self.cycle_amount, self.interval, self.socket_id, self.room_name,
                            debug=bool(os.getenv("DEBUG", 0)))


if __name__ == "__main__":
    redis_cache = redis.Redis(host=os.getenv("REDIS_URL"), port=os.getenv("REDIS_PORT"), db=0)
    client = Client()
    client.health_check_status_code()
    client.sync_time()
    client.measure_timestamps()
