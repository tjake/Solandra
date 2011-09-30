/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package solandra;

import lucandra.CassandraUtils;
import java.io.IOException;
import org.apache.cassandra.service.CassandraDaemon;

/**
 * This class supports two methods for creating a Cassandra node daemon, 
 * invoking the class's main method, and using the jsvc wrapper from 
 * commons-daemon, (for more information on using this class with the 
 * jsvc wrapper, see the 
 * <a href="http://commons.apache.org/daemon/jsvc.html">Commons Daemon</a>
 * documentation).
 */
public class SolandraDaemon implements CassandraDaemon {

    /**
     * Initialize the Cassandra Daemon based on the given <a
     * href="http://commons.apache.org/daemon/jsvc.html">Commons
     * Daemon</a>-specific arguments. To clarify, this is a hook for JSVC.
     * 
     * @param arguments
     *            the arguments passed in from JSVC
     * @throws IOException
     */
    public void init(String[] arguments) throws IOException {
        // Nothing
    }

    /**
     * Start the Cassandra Daemon, assuming that it has already been
     * initialized, via either {@link #init(String[])} or
     * {@link #load(String[])}.
     * 
     * @throws IOException
     */
    public void start() throws IOException {
        startRPCServer();
    }

    /**
     * Stop the daemon, ideally in an idempotent manner.
     */
    public void stop() {
        stopRPCServer();
    }

    /**
     * Clean up all resources obtained during the lifetime of the daemon. Just
     * to clarify, this is a hook for JSVC.
     */
    public void destroy() {
        // Nothing
    }

    @SuppressWarnings("CallToThreadDumpStack")
    public void startRPCServer() {
        String context = System.getProperty("solandra.context", "/solandra");
        try {
            CassandraUtils.startupServer();

            JettySolandraRunner jetty = new JettySolandraRunner(context, CassandraUtils.port, CassandraUtils.webHost);
            jetty.start(false);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void stopRPCServer() {
        CassandraUtils.stopServer();
    }

    public boolean isRPCServerRunning() {
        return true;
    }

    /**
     * A convenience method to initialize and start the daemon in one shot.
     */
    public void activate() {
        startRPCServer();
    }

    /**
     * A convenience method to stop and destroy the daemon in one shot.
     */
    public void deactivate() {
        stopRPCServer();
    }
}
