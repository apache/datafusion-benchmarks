#!/bin/bash
tpctools generate --benchmark tpcds \
  --scale 100 \
  --partitions 12 \
  --generator-path /DSGen-software-code-3.2.0rc1/tools \
  --output /data