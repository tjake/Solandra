/**
 * Copyright 2009 Todd Nine
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
package lucandra;

import java.io.UnsupportedEncodingException;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Iface;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;

/**
 * This class is intended to encapsulate all Cassandra connection information.
 * This class will be used during all operations of an index reader and writer.
 * 
 * @author Todd Nine
 * 
 */
public class IndexContext {

	private final Cassandra.Iface client;
	private final ConsistencyLevel consistencyLevel;
	private final String termColumnFamily;
	private final String documentColumnFamily;
	private final ColumnPath documentPath;
	private final ColumnPath metaColumnPath;

	public IndexContext(Iface client, ConsistencyLevel consistencyLevel,
			String termColumnFamily, String documentColumnFamily) {
		super();
		this.client = client;
		this.consistencyLevel = consistencyLevel;
		this.termColumnFamily = termColumnFamily;
		this.documentColumnFamily = documentColumnFamily;

		try {
			this.documentPath = new ColumnPath(documentColumnFamily)
					.setColumn(CassandraUtils.documentMetaField
							.getBytes("UTF-8"));
			this.metaColumnPath = new ColumnPath(documentColumnFamily)
					.setColumn(CassandraUtils.documentMetaField
							.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			// should never happen
			throw new RuntimeException(e);
		}
	}

	public IndexContext(Iface client, ConsistencyLevel consistencyLevel) {
		this(client, consistencyLevel, "TermInfo", "Documents");
	}

	/**
	 * @return the client
	 */
	public Cassandra.Iface getClient() {
		return client;
	}

	/**
	 * @return the consistencyLevel
	 */
	public ConsistencyLevel getConsistencyLevel() {
		return consistencyLevel;
	}

	/**
	 * @return the termColumnFamily
	 */
	public String getTermColumnFamily() {
		return termColumnFamily;
	}

	/**
	 * @return the documentColumnFamily
	 */
	public String getDocumentColumnFamily() {
		return documentColumnFamily;
	}

	/**
	 * 
	 * @return
	 */
	public ColumnPath getDocumentColumnPath() {
		return documentPath;
	}

	/**
	 * 
	 * @return
	 */
	public ColumnPath getMetaColumnPath() {
		return metaColumnPath;
	}

}
