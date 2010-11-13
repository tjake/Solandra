#!/bin/sh

mkdir data
cd data
curl http://kdd.ics.uci.edu/databases/reuters21578/reuters21578.tar.gz | tar -zxf -
echo "Data downloaded, now run ./2-import-data.sh"
