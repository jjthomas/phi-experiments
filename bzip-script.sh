#!/bin/bash
for i in `seq 0 $1`; do
  echo $i
  bzip2 -1c kafka-json.txt > kafka-json.txt.bzip2.$i &
done
wait
