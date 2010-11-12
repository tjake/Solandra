#!/bin/sh

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
#Increase this to increase the number of shards loaded at once
#
SHARDS_AT_ONCE="1"

#
#Log to stderr
#
USE_STDERR=$1

if [ "x$SOLANDRA_INCLUDE" = "x" ]; then
    for include in /usr/share/solandra/solandra.in.sh \
                   /usr/local/share/solandra/solandra.in.sh \
                   /opt/solandra/solandra.in.sh \
                   ~/.solandra.in.sh \
                   `dirname $0`/solandra.in.sh; do
        if [ -r $include ]; then
            . $include
            break
        fi
    done
elif [ -r $SOLANDRA_INCLUDE ]; then
    . $SOLANDRA_INCLUDE
fi

# Use JAVA_HOME if set, otherwise look for java in PATH
if [ -x $JAVA_HOME/bin/java ]; then
    JAVA=$JAVA_HOME/bin/java
else
    JAVA=`which java`
fi


#debug mode?
if ! test -n "$USE_STDERR"
then
    LOGGING="etc/jetty-logging.xml"
else
    JVM_OPTS="$JVM_OPTS -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=1044"
fi

java $JVM_OPTS -Dshards.at.once=$SHARDS_AT_ONCE -jar start.jar $LOGGING etc/jetty.xml

