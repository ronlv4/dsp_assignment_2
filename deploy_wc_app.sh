#!/bin/bash -xe

./create_step_jars.sh
mvn package
###aws s3 cp target/myWordCount.jar s3://dsp-assignment-2/
hadoop fs -rm -f -R /home/hadoop/outputs/*
hadoop jar target/myWordCount.jar com.dsp.dsp_assignment_2.TestSteps eng
output_dir=/home/hadoop/outputs
for d in $(hadoop fs -ls -C "$output_dir")
do
  echo "$d"
  for f in $(hadoop fs -ls -C "$d" | grep part)
  do
    echo "$f"
    hadoop fs -cat "$f"
  done
done
