/**
 * Copyright Jeff Schmidt
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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.log4j.Logger;
import org.apache.solr.core.SolandraCoreContainer;
import org.apache.solr.core.SolrResourceLoader;

/**
 * {@link SolrResourceLoader} extension that loads core configuration resources
 * from Cassandra. Everything else is delegated to the superclass.
 */
public class SolandraResourceLoader extends SolrResourceLoader
{

	private static final Logger logger = Logger.getLogger(SolandraResourceLoader.class);
	
	protected final String coreName;

	/**
	 * @param instanceDir
	 */
	public SolandraResourceLoader(String coreName, String instanceDir)
	{
		super(instanceDir);
		this.coreName = coreName;
	}

	/*
	 * Overrides superclass method to look in Cassandra to obtain core resources rather
	 * than the file system. If missing in cassandra fallback to FS.
	 */
	@Override
	public InputStream openResource(String resourceName)
	{
		ByteBuffer resourceValue = null;
		
		try
		{
			resourceValue = SolandraCoreContainer.readCoreResource(coreName, resourceName);	
		} 
		catch (Exception e)
		{
			throw new RuntimeException("Error opening " + resourceName, e);
		}
		
		//fallback to solr.home
		if (resourceValue == null)
		{
			return super.openResource(resourceName);
		}
		else
		{
			return  new ByteArrayInputStream(ByteBufferUtil.getArray(resourceValue));
		}		
	}
}
