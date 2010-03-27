#!/bin/bash 

CASSANDRA_HOST=127.0.0.1
CASSANDRA_PORT=9160
CASSANDRA_FRAMED=false

java -cp $( echo *.jar lib/*.jar config | sed 's/ /:/g') \
-Dcassandra.host=${CASSANDRA_HOST} -Dcassandra.port=${CASSANDRA_PORT} -Dcassandra.framed=${CASSANDRA_FRAMED} \
lucandra.demo.BookmarksDemo $*
