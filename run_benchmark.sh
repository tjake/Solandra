#!/bin/bash 

java -Dcassandra.host=127.0.0.1 -Xmx1G -Xms256M -cp $( echo *.jar lib/*.jar config/fat_client config| sed 's/ /:/g') lucandra.benchmarks.BenchmarkTest $*
