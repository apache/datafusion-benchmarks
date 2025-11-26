#!/bin/bash
cd /dsgen-tools/tools

for i in $(seq 1 12); do
  mkdir -p /data/part_$i
  ./dsdgen -scale 100 \
           -dir /data/part_$i \
           -parallel 12 \
           -child $i \
           -terminate n &
done
wait