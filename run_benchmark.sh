#!/bin/bash 

java -ea -Xmx2G -Xms256M -cp $( echo *.jar lib/*.jar config config| sed 's/ /:/g') lucandra.benchmarks.BenchmarkTest $*
