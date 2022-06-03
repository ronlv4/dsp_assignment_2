#!/bin/bash -xe

files_count=$1
output_dir=`hadoop fs -ls /home/hadoop/outputs | tail -n${files_count} | cut -b 62-`
hadoop fs -copyToLocal ${output_dir} '/home/hadoop/dsp_assignment_2/outputs/'
