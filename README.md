Solandra
========
Solandra is a real-time distributed search engine built on [Apache Solr](http://lucene.apache.org) and [Apache Cassandra](http://cassandra.apache.org).
It makes managing and dynamically growing Solr simple(r). 

####Requirements:####

Java >= 1.6

####Features:######

  - Supports most out-of-the-box Solr functionality (search, faceting, highlights)
  - Multi-master (read/write to any node)
  - Writes become instantly available
  - Easily add new SolrCores w/o restart 
  - Replication, Sharding, Caching and Compaction managed by Cassandra

####Getting started:####

The following will guide you through setting up a single node instance of Solandra.

From the Solandra base directory:
  
  - mkdir /tmp/cassandra
  - ant
  - cd solandra-app; run_server.sh &
  - $CASSANDRA_HOME/bin/cassandra-cli --host=localhost --port=9160 --batch < schema.cml
  
Now that Solandra is running you can run the demo
  
  - cd reuters-demo
  - ./1-download_data.sh
  - ./2-import_data.sh  
  - open website/index.html in your favorite browser 
