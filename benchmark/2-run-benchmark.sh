
#!/bin/bash 

java -ea -Xmx1G -Xms256M -cp $( echo ../*.jar ../lib/*.jar ../resources | sed 's/ /:/g') lucandra.benchmarks.BenchmarkTest $*
