#!/bin/bash 
java -cp lucandra.jar:lucandra-tests.jar:lib/libthrift.jar:lib/log4j-1.2.15.jar:\
lib/lucene-core-2.4.1.jar:lib/slf4j-api-1.5.8.jar:lib/slf4j-log4j12-1.5.8.jar:config lucandra.demo.BookmarksDemo $*
