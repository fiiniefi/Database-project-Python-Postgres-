from .api.postgres_api import PostgresAPI
import json
from ast import literal_eval
from functools import reduce


def flatten(input_list):
    return reduce(lambda x, y: x + y, input_list)


def name_kwargs_map(jsons):
    return flatten([list(single_json.items()) for single_json in jsons])


json_filepath = "input_file"

with PostgresAPI('szpp', 'init', 'qwerty', 'localhost') as api:
    with open(json_filepath) as json_file:
        content = json_file.read()
        jsons = [json.loads(content)] if isinstance(content, dict) else \
                [json.loads(single_json) for single_json in literal_eval(content)]
    output = [getattr(api, pair[0])(**pair[1])
              for pair in
              name_kwargs_map(jsons)]
    print(output)
