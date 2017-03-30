#!/usr/bin/env bash

hadoop fs -rm -r out tmp >/dev/null 2>&1
hadoop jar jar/hw2.jar ru.mipt.hobod.hw2.job1_join.JobOneMain /data/stackexchange/posts /data/stackexchange/users out >/dev/null 2>&1
hadoop jar jar/hw2.jar ru.mipt.hobod.hw2.job2_grouping.JobTwoMain out tmp

