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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.thrift.Cassandra.Iface;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.junit.Test;

/**
 * Tests persistence and query ranges on numeric types
 * 
 * @author Todd Nine
 * 
 */
public class NumericRangeTests extends LucandraTestHelper {

	private static final String LONG = "long";
	private static final String STRING1 = "str1";
	private static final String STRING2 = "str2";

	private static final String STRING1_VAL = "testval1";
	private static final String STRING2_VAL = "testval2";

	private static Iface connection;
	private static Document first;
	private static Document second;
	private static Document third;
	private static long low;
	private static long mid;
	private static long high;
	private static String indexName = Long.toHexString(System
			.currentTimeMillis());

	static {
		try {

			setupServer();
			connection = CassandraUtils.createConnection();

			// clean up indexes before we run our test

			low = 1277266160637L;
			mid = low + 1000;
			high = mid + 1000;

			first = new Document();
			first.add(new Field("Id", "first", Store.YES, Index.ANALYZED));

			NumericField numeric = new NumericField(LONG, Store.YES, true);
			numeric.setLongValue(low);
			first.add(numeric);
			first.add(new Field(STRING1, STRING1_VAL, Store.NO,
					Index.NOT_ANALYZED));
			first.add(new Field(STRING2, STRING2_VAL, Store.NO,
					Index.NOT_ANALYZED));

			second = new Document();
			second.add(new Field("Id", "second", Store.YES, Index.ANALYZED));

			numeric = new NumericField(LONG, Store.YES, true);
			numeric.setLongValue(mid);
			second.add(numeric);
			second.add(new Field(STRING1, STRING1_VAL, Store.NO,
					Index.NOT_ANALYZED));
			second.add(new Field(STRING2, STRING2_VAL, Store.NO,
					Index.NOT_ANALYZED));

			third = new Document();
			third.add(new Field("Id", "third", Store.YES, Index.ANALYZED));

			numeric = new NumericField(LONG, Store.YES, true);
			numeric.setLongValue(high);
			third.add(numeric);
			third.add(new Field(STRING1, STRING1_VAL, Store.NO,
					Index.NOT_ANALYZED));
			third.add(new Field(STRING2, STRING2_VAL, Store.NO,
					Index.NOT_ANALYZED));

			IndexWriter writer = new IndexWriter(indexName, context);
			// writer.setAutoCommit(false);

			SimpleAnalyzer analyzer = new SimpleAnalyzer();

			writer.addDocument(first, analyzer);
			writer.addDocument(second, analyzer);
			writer.addDocument(third, analyzer);

			// writer.commit();

		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	@Test
	public void testLongRangeInclusive() throws Exception {

		NumericRangeQuery query = NumericRangeQuery.newLongRange(LONG, mid,
				null, true, true);

		IndexReader reader = new IndexReader(indexName, context);

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

		// now we'll query from the middle inclusive

		NumericRangeQuery query = NumericRangeQuery.newLongRange(LONG, mid,
				null, false, true);

		IndexReader reader = new IndexReader(indexName, context);

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
	public void testLongRangeLessExclusive() throws Exception {

		// now we'll query from the middle inclusive

		NumericRangeQuery query = NumericRangeQuery.newLongRange(LONG, null,
				mid, true, false);

		IndexReader reader = new IndexReader(indexName, context);

		IndexSearcher searcher = new IndexSearcher(reader);

		TopDocs docs = searcher.search(query, 1000);

		assertEquals(1, docs.totalHits);

		Set<String> results = new HashSet<String>();

		for (ScoreDoc doc : docs.scoreDocs) {
			Document returned = searcher.doc(doc.doc);
			results.add(returned.get("Id"));
		}

		assertTrue(results.contains("first"));

	}

	@Test
	public void testLongRangeLessInclusive() throws Exception {

		// now we'll query from the middle inclusive

		NumericRangeQuery query = NumericRangeQuery.newLongRange(LONG, null,
				mid, true, true);

		IndexReader reader = new IndexReader(indexName, context);

		IndexSearcher searcher = new IndexSearcher(reader);

		TopDocs docs = searcher.search(query, 1000);

		assertEquals(2, docs.totalHits);

		Set<String> results = new HashSet<String>();

		for (ScoreDoc doc : docs.scoreDocs) {
			Document returned = searcher.doc(doc.doc);
			results.add(returned.get("Id"));
		}

		assertTrue(results.contains("first"));
		assertTrue(results.contains("second"));

	}

	@Test
	public void testLongRangeMinValueAll() throws Exception {

		// now we'll query from the middle inclusive

		NumericRangeQuery query = NumericRangeQuery.newLongRange(LONG,
				Long.MIN_VALUE, null, true, true);

		IndexReader reader = new IndexReader(indexName, context);

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

	@Test
	public void testLongRangeMaxAll() throws Exception {

		// now we'll query from the middle inclusive

		NumericRangeQuery query = NumericRangeQuery.newLongRange(LONG, null,
				Long.MAX_VALUE, true, true);

		IndexReader reader = new IndexReader(indexName, context);

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

	@Test
	public void testLongRangeZeroAll() throws Exception {

		// now we'll query from the middle inclusive

		NumericRangeQuery query = NumericRangeQuery.newLongRange(LONG, 1L,
				null, true, true);

		IndexReader reader = new IndexReader(indexName, context);

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

	@Test
	public void testDocumentOrdering() throws Exception {

		// now we'll query from the middle inclusive

		BooleanQuery query = new BooleanQuery();

		query.add(new TermQuery(new Term(STRING1, STRING1_VAL)), Occur.SHOULD);

		query.add(new TermQuery(new Term(STRING2, "randomval")), Occur.SHOULD);

		// sort by long descending
		SortField sort = new SortField(LONG, SortField.LONG, true);

		IndexReader reader = new IndexReader(indexName, context);

		IndexSearcher searcher = new IndexSearcher(reader);

		TopDocs docs = searcher.search(query, null, 1000, new Sort(sort));

		assertEquals(3, docs.totalHits);

		List<String> results = new ArrayList<String>();

		for (ScoreDoc doc : docs.scoreDocs) {
			Document returned = searcher.doc(doc.doc);
			results.add(returned.get("Id"));
		}

		assertEquals("third", results.get(0));
		assertEquals("second", results.get(1));
		assertEquals("first", results.get(2));

	}

	@Test
	public void testDocumentOrderingSearchTwice() throws Exception {

		// now we'll query from the middle inclusive

		BooleanQuery query = new BooleanQuery();

		query.add(new TermQuery(new Term(STRING1, STRING1_VAL)), Occur.SHOULD);

		query.add(new TermQuery(new Term(STRING2, "randomval")), Occur.SHOULD);

		// sort by long descending
		SortField sort = new SortField(LONG, SortField.LONG, true);

		IndexReader reader = new IndexReader(indexName, context);

		IndexSearcher searcher = new IndexSearcher(reader);

		TopDocs docs = searcher.search(query, null, 1000, new Sort(sort));

		assertEquals(3, docs.totalHits);

		List<String> results = new ArrayList<String>();

		for (ScoreDoc doc : docs.scoreDocs) {
			Document returned = searcher.doc(doc.doc);
			results.add(returned.get("Id"));
		}

		assertEquals("third", results.get(0));
		assertEquals("second", results.get(1));
		assertEquals("first", results.get(2));

		query = new BooleanQuery();

		query.add(new TermQuery(new Term(STRING1, "fooval")), Occur.SHOULD);

		query.add(new TermQuery(new Term(STRING2, STRING2_VAL)), Occur.SHOULD);

		docs = searcher.search(query, null, 1000, new Sort(sort));

		assertEquals(3, docs.totalHits);

		results = new ArrayList<String>();

		for (ScoreDoc doc : docs.scoreDocs) {
			Document returned = searcher.doc(doc.doc);
			results.add(returned.get("Id"));
		}

		assertEquals("third", results.get(0));
		assertEquals("second", results.get(1));
		assertEquals("first", results.get(2));

	}

}