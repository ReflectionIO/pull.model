#!/bin/bash
# Grabs and kill a process from the pidlist that has the word pull.model

pid=`ps aux | grep pull.model | awk '{print $2}'`
kill -9 $pid
