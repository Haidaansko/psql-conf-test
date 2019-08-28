import json
import pathlib


table = []
for fname in pathlib.Path('./out/').iterdir():
    name = fname.name.split('.')[0]
    mt, pg_conf = name.split('_')
    with open(fname.absolute(), 'r') as f:
        for line in f.readlines():
            query, query_t = line.split()
            query_t = float(query_t)
            table.append([mt, pg_conf, query, query_t])


with open('info.json', 'w') as f:
    json.dump(table, f)
