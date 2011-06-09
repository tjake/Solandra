/**
 * Copyright T Jake Luciani
 * 
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


public class SolandraServer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		String context = System.getProperty("solandra.context", "/solandra");
		
		int port = Integer.parseInt(System.getProperty("solandra.port", "8983"));
		
		try {
		    
		    if(System.getProperty("solandra.clientmode", "false").equalsIgnoreCase("true"))
		        CassandraUtils.startupClient();
		    else
		        CassandraUtils.startupServer();

			JettySolandraRunner jetty = new JettySolandraRunner(context, port);
			jetty.start(false);
		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

}
