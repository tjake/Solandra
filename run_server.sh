#!/bin/bash 

REDIS_HOST=localhost

java -Xmx1G -cp $( echo ../cassandra-trunk/lib/*.jar *.jar lib/*.jar config | sed 's/ /:/g') \
-Dredis.host=${REDIS_HOST} -Dsolr.solr.home=solr-example/solr solandra.JettySolandraRunner $*
