#!/bin/bash -xe

mvn clean
mvn package
mv target/dsp_assignment_2-1.0-SNAPSHOT.jar target/myWordCount.jar
aws s3 cp target/myWordCount.jar s3://dsp-assignment-2/
