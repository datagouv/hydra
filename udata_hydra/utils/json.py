import json


def is_json_file(filename: str) -> bool:
    """Checks if a string is a valid json"""
    try:
        with open(filename) as f:
            json.load(f)
            return True
    except ValueError:
        return False
