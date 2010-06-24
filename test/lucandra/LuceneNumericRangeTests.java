/**
 * 
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
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Todd Nine
 * 
 */
public class LuceneNumericRangeTests {
	private static final int LONG_PRECISION = 6;

	private static Document first;
	private static Document second;
	private static Document third;
	private static long low;
	private static long mid;
	private static long high;
	
	private static org.apache.lucene.index.IndexReader reader;



	@BeforeClass
	public static void writeIndexes() throws TTransportException,
			CorruptIndexException, IOException {
		// clean up indexes before we run our test
		cleanIndexes();

		low = 1277266160637l;
		mid = low + 1000;
		high = low + 1000;

		first = new Document();
		first.add(new Field("Id", "first", Store.YES, Index.ANALYZED));

		NumericField numeric = new NumericField("long", LONG_PRECISION,
				Store.YES, true);
		numeric.setLongValue(low);
		first.add(numeric);

		second = new Document();
		second.add(new Field("Id", "second", Store.YES, Index.ANALYZED));

		numeric = new NumericField("long", LONG_PRECISION, Store.YES, true);
		numeric.setLongValue(mid);
		second.add(numeric);

		third = new Document();
		third.add(new Field("Id", "third", Store.YES, Index.ANALYZED));

		numeric = new NumericField("long", LONG_PRECISION, Store.YES, true);
		numeric.setLongValue(high);
		third.add(numeric);
		
		org.apache.lucene.index.IndexWriter writer =new org.apache.lucene.index.IndexWriter(new RAMDirectory(), new StandardAnalyzer(Version.LUCENE_CURRENT), MaxFieldLength.UNLIMITED);
		reader = writer.getReader();

		SimpleAnalyzer analyzer = new SimpleAnalyzer();

		writer.addDocument(first, analyzer);
		writer.addDocument(second, analyzer);
		writer.addDocument(third, analyzer);

		writer.commit();

	}

	@AfterClass
	public static void cleanIndexes() throws CorruptIndexException,
			IOException, TTransportException {

//		IndexWriter writer = new IndexWriter("longvals", CassandraUtils
//				.createConnection(), ConsistencyLevel.ONE);
//		writer.deleteDocuments(new Term("Id", "first"));
//		writer.deleteDocuments(new Term("Id", "second"));
//		writer.deleteDocuments(new Term("Id", "third"));
	}

	@Test
	public void testLongRangeInclusive() throws Exception {

		NumericRangeQuery query = NumericRangeQuery.newLongRange("long",
				LONG_PRECISION, mid, null, true, true);

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

		NumericRangeQuery query = NumericRangeQuery.newLongRange("long",
				LONG_PRECISION, mid, null, false, true);

		
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

		NumericRangeQuery query = NumericRangeQuery.newLongRange("long",
				LONG_PRECISION, null, mid, true, false);

		
		IndexSearcher searcher = new IndexSearcher(reader);

		TopDocs docs = searcher.search(query, 1000);

		assertEquals(1, docs.totalHits);

		Set<String> results = new HashSet<String>();

		for (ScoreDoc doc : docs.scoreDocs) {
			Document returned = searcher.doc(doc.doc);
			results.add(returned.get("Id"));
		}

		assertTrue(results.contains("one"));

	}

	@Test
	public void testLongRangeLessInclusive() throws Exception {

		// now we'll query from the middle inclusive

		NumericRangeQuery query = NumericRangeQuery.newLongRange("long",
				LONG_PRECISION, null, mid, true, true);

		
		IndexSearcher searcher = new IndexSearcher(reader);

		TopDocs docs = searcher.search(query, 1000);

		assertEquals(2, docs.totalHits);

		Set<String> results = new HashSet<String>();

		for (ScoreDoc doc : docs.scoreDocs) {
			Document returned = searcher.doc(doc.doc);
			results.add(returned.get("Id"));
		}

		assertTrue(results.contains("one"));
		assertTrue(results.contains("two"));

	}

	@Test
	public void testLongRangeZeroAll() throws Exception {

		// now we'll query from the middle inclusive

		NumericRangeQuery query = NumericRangeQuery.newLongRange("long",
				LONG_PRECISION, (long) 0, null, true, true);

		
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

		NumericRangeQuery query = NumericRangeQuery.newLongRange("long",
				LONG_PRECISION, null, high * 1000, true, true);

	
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
