#!/bin/sh


SCHEMA_URL=http://localhost:8983/solandra/schema/reuters
SCHEMA=schema.xml

curl $SCHEMA_URL --data-binary @$SCHEMA -H 'Content-type:text/xml; charset=utf-8' 

echo "Posted $SCHEMA to $SCHEMA_URL"
sleep 1

echo "Loading data to solandra, note: this importer uses a slow xml parser"
sleep 1
java -jar reutersimporter.jar http://localhost:8983/solandra/reuters data

echo "Data loaded, now open ./website/index.html in your favorite browser!"