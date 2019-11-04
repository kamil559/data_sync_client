import ast
import json


def get_json(json_bytes: bytes) -> dict:
    return json.loads(json_bytes)


def parse_message_data(message_data: str) -> dict:
    return ast.literal_eval(message_data)