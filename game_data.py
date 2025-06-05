import json
import os
from pathlib import Path

import requests
import urllib3.util
from websockets import Subprotocol
from websockets.exceptions import WebSocketException
from websockets.sync.client import connect

uri = '{scheme}://{host}/v1/database/{module}/{endpoint}'
proto = Subprotocol('v1.json.spacetimedb')


def dump_tables(host, module, queries, auth=None):
    save_data = {}
    if isinstance(queries, str):
        queries = [queries]
    try:
        with connect(
                uri.format(scheme='wss', host=host, module=module, endpoint='subscribe'),
                # user_agent_header=None,
                additional_headers={"Authorization": auth} if auth else {},
                subprotocols=[proto],
                max_size=None,
                max_queue=None
        ) as ws:
            ws.recv()
            sub = json.dumps(dict(Subscribe=dict(
                request_id=1,
                query_strings=[
                    f'SELECT * FROM {q};' if isinstance(q, str) else
                    f'SELECT * FROM {q[0]} WHERE {q[1]} = {q[2]};'
                    for q in queries
                ]
            )))
            ws.send(sub)
            for msg in ws:
                data = json.loads(msg)
                if 'InitialSubscription' in data:
                    initial = data['InitialSubscription']['database_update']['tables']
                    for table in initial:
                        name = table['table_name']
                        rows = table['updates'][0]['inserts']
                        save_data[name] = [json.loads(row) for row in rows]
                    break
    except WebSocketException as ex:
        raise ex

    return save_data


def get_schema(host, module):
    target = uri.format(scheme='https', host=host, module=module, endpoint='schema')
    res = requests.get(target, params=dict(version=9))
    return res.json() if res.status_code == 200 else None


def load_tables_names(table_file):
    with open(table_file, 'r') as f:
        return [t.strip() for t in f.readlines() if t.strip()]


def get_region_info(global_host, auth):
    res = dump_tables(global_host, 'bitcraft-global', 'region_connection_info', auth)
    obj = res['region_connection_info'][-1]
    return urllib3.util.parse_url(obj['host']).host, obj['module']


def save_tables(data_dir, subdir, tables):
    root = data_dir / subdir
    root.mkdir(exist_ok=True)
    for name, data in tables.items():
        with open(root / (name + '.json'), 'w') as f:
            json.dump(data, fp=f, indent=2)
    pass


def main():
    data_dir = Path(os.getenv('DATA_DIR') or 'server')
    data_dir.mkdir(exist_ok=True)
    global_host = os.getenv('BITCRAFT_SPACETIME_HOST')
    if not global_host:
        raise ValueError('BITCRAFT_SPACETIME_HOST not set')
    auth = os.getenv('BITCRAFT_SPACETIME_AUTH') or None

    region_host, region_module = get_region_info(global_host, auth)

    schema = get_schema(region_host, region_module)
    if schema:
        with open(data_dir / 'schema.json', 'w') as f:
            json.dump(schema, fp=f, indent=2)

    curr_dir = Path(__file__).parent.resolve()
    global_tables = load_tables_names(curr_dir / 'global_tables.txt')
    region_tables = load_tables_names(curr_dir / 'region_tables.txt')

    if global_tables:
        global_res = dump_tables(global_host, 'bitcraft-global', global_tables, auth)
        save_tables(data_dir, 'global', global_res)

    if region_tables:
        region_res = dump_tables(region_host, region_module, region_tables, auth)
        save_tables(data_dir, 'region', region_res)


if __name__ == '__main__':
    main()
