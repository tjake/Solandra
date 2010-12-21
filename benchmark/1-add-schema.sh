#!/bin/sh

SCHEMA_URL=http://localhost:8983/solandra/schema/bench
SCHEMA=schema.xml

curl $SCHEMA_URL --data-binary @$SCHEMA -H 'Content-type:text/xml; charset=utf-8' 

echo "Posted $SCHEMA to $SCHEMA_URL"
