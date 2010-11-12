#!/bin/bash 

java -ea -Xmx1G -Xms256M -cp $( echo solr-resources *.jar lib/*.jar | sed 's/ /:/g') lucandra.wikipedia.WikipediaImporter $*
