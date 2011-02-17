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


# Parse any command line options.
args=`getopt fbhdp: "$@"`
eval set -- "$args"

while true; do
    case "$1" in
        -p)
            pidfile="$2"
            shift 2
        ;;
        -f)
            foreground="yes"
            shift
        ;;
        -h)
            echo "Usage: $0 [-f] [-h] [-b] [-d] [-s num] [-p pidfile]"
	    echo "  -f : run in foreground"
	    echo "  -h : prints this help message"
	    echo "  -b : adds solandra's cassandra schema"
	    echo "  -d : enables debuging port and logs to the foreground"
	    echo "  -s : specify the number of shards to write to at once (default: 2)"
            exit 0
        ;;
        -b)
            schema="yes"
            shift
        ;;
        -d)
            debug="yes"
            shift
        ;;
        --)
            shift
            break
        ;;
        *)
            echo "Error parsing arguments! $1 $2" >&2
            exit 1
        ;;
    esac
done

#boostrap schema?
if [ "x$schema" != "x" ]
then
    foreground=""
    debug="" 
fi

#debug mode?
if [ "x$debug" != "x" ]
then
    foreground="yes"
    JVM_OPTS="$JVM_OPTS -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=1044"
else
    LOGGING="etc/jetty-logging.xml"
fi

if [ "x$pidpath" != "x" ]; then
    solandra_parms="$solandra_parms -Dcassandra-pidfile=$pidpath"
fi

solandra_parms="$solandra_parms -Dlog4j.configuration=log4j.properties -Dlog4j.defaultInitOverride=true"
    
# The solandra-foreground option will tell Cassandra not
# to close stdout/stderr, but it's up to us not to background.
if [ "x$foreground" != "x" ]; then
    solandra_parms="$solandra_parms -Dcassandra-foreground=yes"
    exec $JAVA $JVM_OPTS $solandra_parms -jar start.jar $LOGGING etc/jetty.xml
# Startup Solandra, background it, and write the pid.
else
    exec $JAVA $JVM_OPTS $solandra_parms -jar start.jar $LOGGING etc/jetty.xml <&- &
    [ ! -z $pidfile ] && printf "%d" $! > $pidfile
fi

if [ "x$schema" != "x" ]
then
    sleep 1
    echo "Waiting 10 seconds for solandra to start before bootstrapping schema..."
    sleep 10
    cd cassandra-tools && ./cassandra-cli --host localhost < solandra.cml
    echo "Solandra ready"
fi
