import json


def file2dict(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)
