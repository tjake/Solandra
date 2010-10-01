#!/bin/bash 

java -ea -Xmx1G -Xms256M -Djava.util.logging.config.file=config/log4j.properties -cp $( echo config *.jar lib/*.jar | sed 's/ /:/g') lucandra.wikipedia.WikipediaImporter $*
