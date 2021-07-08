#!/usr/bin/env bash

exec &> job_log.txt  # log stderr and stdout to created file 'job_log.txt'

spark-submit autoinc_spark.py