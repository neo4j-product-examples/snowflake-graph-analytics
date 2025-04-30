import os
import pandas as pd
import pathlib

base_path = pathlib.Path(__file__).resolve().parent
nodes_path = base_path / "nodedfs"
rels_path = base_path / "reldfs"

def load_to_pandas():
    nodedfs = {}
    for root, dirs, files in os.walk(nodes_path):
        for file in files:
            if file.endswith(".gzip"):
                node_label = file.split(".")[0]
                nodedfs[node_label] = pd.read_parquet(os.path.join(root, file))
    reldfs = {}
    for root, dirs, files in os.walk(rels_path):
        for file in files:
            if file.endswith(".gzip"):
                rel_label = file.split(".")[0]
                reldfs[rel_label] = pd.read_parquet(os.path.join(root, file))
    return nodedfs, reldfs

