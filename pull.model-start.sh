#!/bin/bash
# set path
PATH=/usr/local/bin:/usr/bin:/bin:/opt/java/jdk1.7.0_55:/opt/java/jdk1.7.0_55/bin
cd /opt/pull.model
# running on compute engine, lease items for an hour, lease 10 items at a time
java -jar -Djava.util.logging.config.file=logging.properties /opt/pull.model/pull.model.jar true 3600 10 >> /opt/pull.model/logs/stdouterr.log 2>&1
