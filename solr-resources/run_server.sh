#!/bin/bash 

REDIS_HOST=localhost
MAX_HEAP_SIZE="2G" 


# Here we create the arguments that will get passed to the jvm when
# starting cassandra.
JVM_OPTS="$JVM_OPTS -ea"

JVM_OPTS="$JVM_OPTS -Xms256M"
JVM_OPTS="$JVM_OPTS -Xmx$MAX_HEAP_SIZE"
JVM_OPTS="$JVM_OPTS -Xss128k" 

JVM_OPTS="$JVM_OPTS -XX:+UseParNewGC" 
JVM_OPTS="$JVM_OPTS -XX:+UseConcMarkSweepGC" 
JVM_OPTS="$JVM_OPTS -XX:+CMSParallelRemarkEnabled" 
JVM_OPTS="$JVM_OPTS -XX:SurvivorRatio=8" 
JVM_OPTS="$JVM_OPTS -XX:MaxTenuringThreshold=1" 
JVM_OPTS="$JVM_OPTS -XX:+HeapDumpOnOutOfMemoryError" 

JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.port=8080" 
JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.ssl=false" 
JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.authenticate=false" 

java $JVM_OPTS -jar start.jar
