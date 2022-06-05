#!/bin/bash -xe


mvn clean
mvn package
mv target/dsp_assignment_2-1.0-SNAPSHOT.jar target/myWordCount.jar
###aws s3 cp target/myWordCount.jar s3://dsp-assignment-2/
hadoop fs -rm -f -R /home/hadoop/outputs/*
hadoop jar target/myWordCount.jar com.dsp.dsp_assignment_2.TestSteps
output_dir=$(hadoop fs -ls /home/hadoop/outputs | awk '{printf $8}')
hadoop fs -cat "${output_dir}"/*