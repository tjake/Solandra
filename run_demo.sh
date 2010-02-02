#!/bin/bash 

CASSANDRA_HOST=127.0.0.1
CASSANDRA_PORT=9160
CASSANDRA_FRAMED=true

java -cp lucandra.jar:lucandra-tests.jar:lib/libthrift-r820831.jar:lib/log4j-1.2.15.jar:\
lib/lucene-core-2.4.1.jar:lib/slf4j-api-1.5.8.jar:lib/slf4j-log4j12-1.5.8.jar:config \
-Dcassandra.host=${CASSANDRA_HOST} -Dcassandra.port=${CASSANDRA_PORT} -Dcassandra.framed=${CASSANDRA_FRAMED} \
lucandra.demo.BookmarksDemo $*
