import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
from uuid import uuid4
import argparse

# Make a tall dataset
def gen_batches(n_groups=20, rows_per_group=200_000):
    for _ in range(n_groups):
        id_val = uuid4().bytes
        yield pa.table({
            "x": np.random.random(rows_per_group), # This will compress poorly
            "y": np.random.random(rows_per_group),
            "a": pa.array(list(range(rows_per_group)), type=pa.int32()), # This compresses with delta encoding
            "id": pa.array([id_val] * rows_per_group), # This compresses with RLE
        })

schema = pa.schema({"x": pa.float64(), "y": pa.float64(), "a": pa.int32(), "id": pa.binary()})

parser = argparse.ArgumentParser()
parser.add_argument("--rows-per-group", type=int, default=200_000)
parser.add_argument("--n-groups", type=int, default=10)
args = parser.parse_args()

with pq.ParquetWriter('tall.parquet', schema)  as writer:
    for batch in gen_batches(args.n_groups, args.rows_per_group):
        writer.write_table(batch)
