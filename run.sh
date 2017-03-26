#!/usr/bin/env bash

hadoop fs -rm -r out >/dev/null 2>&1
hadoop jar jar/UrlCount.jar ru.mipt.hobod.hw2.MainClass /data/stackexchange/posts /data/stackexchange/users out >/dev/null 2>&1
hadoop fs -cat out/part-r-00000 | head -40