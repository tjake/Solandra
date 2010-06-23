/**
 * Copyright 2010 Todd Nine
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests persistence and query ranges on numeric types
 * 
 * @author Todd Nine
 * 
 */
public class NumericRangeTests {

	private Cassandra.Iface connection;
	private Document first;
	private Document second;
	private Document third;
	private long midpoint;

	@Before
	public void setup() throws TTransportException {
		connection = CassandraUtils.createConnection();

		midpoint = 1001;

		first = new Document();
		first.add(new Field("Id", "first", Store.YES, Index.NOT_ANALYZED));

		NumericField numeric = new NumericField("long", Integer.MAX_VALUE,
				Store.YES, true);
		numeric.setLongValue(1000);
		first.add(numeric);

		second = new Document();
		second.add(new Field("Id", "second", Store.YES, Index.NOT_ANALYZED));

		numeric = new NumericField("long", Integer.MAX_VALUE, Store.YES, true);
		numeric.setLongValue(midpoint);
		second.add(numeric);

		third = new Document();
		third.add(new Field("Id", "third", Store.YES, Index.NOT_ANALYZED));

		numeric = new NumericField("long", Integer.MAX_VALUE, Store.YES, true);
		numeric.setLongValue(1002);
		third.add(numeric);
	}

	@After
	public void cleanup() throws CorruptIndexException, IOException {
		IndexWriter writer = new IndexWriter("longvals", connection,
				ConsistencyLevel.ONE);
		writer.deleteDocuments(new Term("Id", "first"));
		writer.deleteDocuments(new Term("Id", "second"));
		writer.deleteDocuments(new Term("Id", "third"));
	}

	@Test
	public void testLongRangeInclusive() throws Exception {

		IndexWriter writer = new IndexWriter("longvals", connection,
				ConsistencyLevel.ONE);
		writer.setAutoCommit(false);

		SimpleAnalyzer analyzer = new SimpleAnalyzer();

		writer.addDocument(first, analyzer);
		writer.addDocument(second, analyzer);
		writer.addDocument(third, analyzer);

		writer.commit();

		// now we'll query from the middle inclusive

		NumericRangeQuery query = NumericRangeQuery.newLongRange("long",
				Integer.MAX_VALUE, midpoint, null, true, true);

		IndexReader reader = new IndexReader("longvals", connection);

		IndexSearcher searcher = new IndexSearcher(reader);

		TopDocs docs = searcher.search(query, 1000);

		assertEquals(2, docs.totalHits);

		Set<String> results = new HashSet<String>();

		for (ScoreDoc doc : docs.scoreDocs) {
			Document returned = searcher.doc(doc.doc);
			results.add(returned.get("Id"));
		}

		assertTrue(results.contains("second"));

		assertTrue(results.contains("third"));

	}

	@Test
	public void testLongRangeExclusive() throws Exception {

		IndexWriter writer = new IndexWriter("longvals", connection,
				ConsistencyLevel.ONE);
		writer.setAutoCommit(false);

		SimpleAnalyzer analyzer = new SimpleAnalyzer();

		writer.addDocument(first, analyzer);
		writer.addDocument(second, analyzer);
		writer.addDocument(third, analyzer);

		writer.commit();

		// now we'll query from the middle inclusive

		NumericRangeQuery query = NumericRangeQuery.newLongRange("long",
				Integer.MAX_VALUE, midpoint, null, false, true);

		IndexReader reader = new IndexReader("longvals", connection);

		IndexSearcher searcher = new IndexSearcher(reader);

		TopDocs docs = searcher.search(query, 1000);

		assertEquals(1, docs.totalHits);

		Set<String> results = new HashSet<String>();

		for (ScoreDoc doc : docs.scoreDocs) {
			Document returned = searcher.doc(doc.doc);
			results.add(returned.get("Id"));
		}

		assertTrue(results.contains("third"));

	}

	@Test
	public void testLongRangeAll() throws Exception {

		IndexWriter writer = new IndexWriter("longvals", connection,
				ConsistencyLevel.ONE);
		writer.setAutoCommit(false);

		SimpleAnalyzer analyzer = new SimpleAnalyzer();

		writer.addDocument(first, analyzer);
		writer.addDocument(second, analyzer);
		writer.addDocument(third, analyzer);

		writer.commit();

		// now we'll query from the middle inclusive

		NumericRangeQuery query = NumericRangeQuery.newLongRange("long",
				Integer.MAX_VALUE, (long)0, null, true, true);

		IndexReader reader = new IndexReader("longvals", connection);

		IndexSearcher searcher = new IndexSearcher(reader);

		TopDocs docs = searcher.search(query, 1000);

		assertEquals(3, docs.totalHits);

		Set<String> results = new HashSet<String>();

		for (ScoreDoc doc : docs.scoreDocs) {
			Document returned = searcher.doc(doc.doc);
			results.add(returned.get("Id"));
		}

		assertTrue(results.contains("first"));
		assertTrue(results.contains("second"));
		assertTrue(results.contains("third"));

	}

}
