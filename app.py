import os
import re
from dataclasses import dataclass
from typing import Iterable, Generator, Union, Tuple

import marshmallow_dataclass
import marshmallow

from flask import Flask, request, Response

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")


@dataclass
class Payload:
    file_name: str
    cmd1: str
    value1: str
    cmd2: str
    value2: str


PayloadSchema = marshmallow_dataclass.class_schema(Payload)


def load_data(data: dict) -> Payload:
    try:
        return PayloadSchema().load(data)
    except marshmallow.exceptions.ValidationError:
        raise ValueError


@app.post("/perform_query")
def perform_query() -> Union[Tuple[str, int], Response]:
    req: dict = request.values.to_dict()
    try:
        data_obj = load_data(req)
    except ValueError:
        return '', 400

    if data_obj.file_name not in list(os.walk(DATA_DIR))[-1][-1]:
        return '', 400

    cmd_1 = data_obj.cmd1
    value_1 = data_obj.value1
    cmd_2 = data_obj.cmd2
    value_2 = data_obj.value2

    if not all(x in ['map', 'filter', 'unique', 'sort', 'regex'] for x in [cmd_1, cmd_2]):
        return '', 400

    path = os.path.join(DATA_DIR, data_obj.file_name)

    with open(path, 'r') as f:
        gen: Generator = (raw for raw in f)
        gen1: Iterable[str] = file_processing(cmd_1, value_1, gen)
        gen2: Iterable[str] = file_processing(cmd_2, value_2, gen1)
        result = "\n".join(list(gen2))

    return app.response_class(result, content_type="text/plain")


def file_processing(cmd: str, value: str, gen: Iterable[str]) -> Iterable[str]:
    if cmd == 'filter':
        return filter(lambda raw: value in raw, gen)
    elif cmd == 'map':
        return map(lambda x: re.split(' - - | "|" ', x)[int(value)].rstrip(), gen)
    elif cmd == 'unique':
        return set(gen)
    elif cmd == 'sort':
        return sorted(gen, reverse=True if value.lower() == 'asc' else False)
    elif cmd == 'regex':
        return (g for g in gen if re.search(value, g))
    else:
        return gen


if __name__ == "__main__":
    app.run(debug=True)

