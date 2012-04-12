Solandra
========
Solandra is a real-time distributed search engine built on [Apache Solr](http://lucene.apache.org/solr/) and [Apache Cassandra](http://cassandra.apache.org).

At its core, Solandra is a tight integration of Solr and Cassandra, meaning within a single JVM both Solr and Cassandra are running, and
documents are stored and disributed using Cassandra's data model.

Solandra makes managing and dynamically growing Solr simple(r).

For more information please see the [wiki](https://github.com/tjake/Solandra/wiki)

####Project Status:####

Solandra is relatively stable, most of the common functionality used has been working and stable for users.
I personally am no longer developing Solandra much beyond version changes and applying pull requests.

If you are looking for a supported Solr + Cassandra integration beyond what Solandra offers 
look at [DataStax Enterprise Search](http://www.datastax.com/products/enterprise)  
(Full disclosure I am a developer on that team).

I've written up [how Solandra and DataStax Enterprise Search differ here](http://www.datastax.com/dev/blog/cassandra-with-solr-integration-details).

####Requirements:####

Java >= 1.6

####Features:######

  - Supports most out-of-the-box Solr functionality (search, faceting, highlights)
  - Replication, sharding, caching, and compaction managed by Cassandra
  - Multi-master (read/write to any node)
  - Writes become available as soon as write succeeds
  - Easily add new SolrCores w/o restart across the cluster

####Getting started:####

The following will guide you through setting up a single node instance of Solandra.

From the Solandra base directory:

    mkdir /tmp/cassandra-data
    ant
    cd solandra-app; bin/solandra

Now that Solandra is running you can run the demo:

    cd ../../reuters-demo
    ./1-download_data.sh
    ./2-import_data.sh
    While data is loading, open the file ./website/index.html in your favorite browser.


####Embedding in an existing cassandra distribution####

To use an existing Cassandra distribution perform the following steps.

1. Download your Cassandra distribution
2. Unzip it the directory of your choice
3. Run the following solandra ant task to deploy the necessary files into the unzipped dir

    ant -Dcassandra={unzipped dir} cassandra-dist

4. You can now start Solr within Cassandra by using $CASSANDRA_HOME/bin/solandra command. Cassandra now takes two optional properties: -Dsolandra.context and -Dsolandra.port for the context path and the Jetty port.

####Limitations####

Solandra uses Solr's built in distributed searching mechanism.
Most of its limitations are covered here:

[http://wiki.apache.org/solr/DistributedSearch#Distributed_Searching_Limitations](http://wiki.apache.org/solr/DistributedSearch#Distributed_Searching_Limitations)
