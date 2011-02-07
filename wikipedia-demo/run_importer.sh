#!/bin/bash 

java -ea -Xmx1G -Xms256M -cp $( echo ../resources ../*.jar ../lib/*.jar | sed 's/ /:/g') lucandra.wikipedia.WikipediaImporter $*
