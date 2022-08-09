import itertools
import subprocess
import os

row_group_sizes = [1_000, 10_000, 100_000]

# Default: 4096 * 4
buffer_sizes = [None, 4096 * 4, 1024, 4096 * 40]

# Default: 64 * 1024
batch_sizes = [64 * 1024, 1024, 128 * 1024]

os.remove("data.jsonl")

for row_group_size in row_group_sizes:
    subprocess.call(["python", "generate_data.py", "--rows-per-group", str(row_group_size)])

    with open("data.jsonl", "a") as f:
        for (buffer_size, use_threads, batch_size, prebuffer) in \
            itertools.product(buffer_sizes, [True, False], batch_sizes, [True, False]):

            args = ["build/memory_test"]
            if buffer_size is not None:
                args.append("--use-buffered-stream")
                args.append("--buffer-size")
                args.append(str(buffer_size))
            if use_threads:
                args.append("--use-threads")
            args.append("--batch-size")
            args.append(str(batch_size))
            if prebuffer:
                args.append("--pre-buffer")
            args.append("--row-group-size")
            args.append(str(row_group_size))

            subprocess.call(args, stdout=f)
