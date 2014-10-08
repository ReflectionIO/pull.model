#!/bin/bash
# set path
PATH=/usr/local/bin:/usr/bin:/bin:/opt/java/jdk1.7.0_55:/opt/java/jdk1.7.0_55/bin
PULL_MODEL_PATH=/opt/pull.model
cd $PULL_MODEL_PATH
# running on compute engine, lease items for an hour, lease 10 items at a time
java -Dio.reflection.pullmodel.config.file=testenv1.pull.model.properties -Djava.util.logging.config.file=logging.properties -jar $PULL_MODEL_PATH/pull.model.jar true 3600 10 Logger.xml >> $PULL_MODEL_PATH/logs/stdouterr.log 2>&1
