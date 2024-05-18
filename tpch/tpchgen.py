# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import argparse
from datafusion import SessionContext
import subprocess

table_names = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]

def run(cmd: str):
    print(f"Executing: {cmd}")
    subprocess.run(cmd, shell=True)

def convert_tbl_to_parquet(ctx: SessionContext, tbl_filename: str, file_extension: str, parquet_filename: str):
    print(f"Converting {tbl_filename} to {parquet_filename} ...")
    df = ctx.read_csv(tbl_filename, has_header=False, file_extension=file_extension, delimiter="|")
    df.write_parquet(parquet_filename)

def generate_tpch(scale_factor: int, partitions: int):
    if partitions == 1:
        command = f"docker run -v `pwd`/data:/data -it --rm ghcr.io/scalytics/tpch-docker:main -vf -s {scale_factor}"
        run(command)

        # convert to parquet
        ctx = SessionContext()
        for table in table_names:
            convert_tbl_to_parquet(ctx, f"data/{table}.tbl", "tbl", f"data/{table}.parquet")

    else:
        for part in range(1, partitions+1):
            command = f"docker run -v `pwd`/data:/data -it --rm ghcr.io/scalytics/tpch-docker:main -vf -s {scale_factor} -C {partitions} -S {part}"
            run(command)

        # convert to parquet
        ctx = SessionContext()
        for table in table_names:
            run("mkdir -p data/{table}.parquet")
            if table == "nation" or table == "region":
                # nation and region are special cases and do not generate multiple files
                convert_tbl_to_parquet(ctx, f"data/{table}.tbl", "tbl", f"data/{table}.parquet/part1.parquet")
            else:
                for part in range(1, partitions + 1):
                    convert_tbl_to_parquet(ctx, f"data/{table}.tbl.{part}", f"tbl.{part}", f"data/{table}.parquet/part{part}.parquet")


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--scale-factor', type=int, help='The scale factor')
    arg_parser.add_argument('--partitions', type=int, help='The number of partitions')
    args = arg_parser.parse_args()
    generate_tpch(args.scale_factor, args.partitions)