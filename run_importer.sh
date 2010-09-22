#!/bin/bash 

java -ea -Xmx1G -Xms256M -cp $( echo *.jar lib/*.jar config config| sed 's/ /:/g') lucandra.wikipedia.WikipediaImporter $*
