#!/bin/bash 


java -Dcassandra.host=10.176.7.162 -Xmx1G -Xms256M -cp $( echo *.jar lib/*.jar config/fat_client| sed 's/ /:/g') lucandra.wikipedia.WikipediaImporter $*
