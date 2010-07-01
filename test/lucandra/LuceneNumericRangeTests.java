/**
 * 
 */
package lucandra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author Todd Nine
 * 
 */
@Ignore("Ignored until this issue is fixed.  Numeric ranges won't work.  https://issues.apache.org/jira/browse/CASSANDRA-1235")
public class LuceneNumericRangeTests {

	private static Document first;
	private static Document second;
	private static Document third;
	private static long low;
	private static long mid;
	private static long high;
	
	private static org.apache.lucene.index.IndexReader reader;



	@BeforeClass
	public static void writeIndexes() throws
			CorruptIndexException, IOException {
	

		low = 1277266160637L;
		mid = low + 1000;
		high = mid + 1000;

		first = new Document();
		first.add(new Field("Id", "first", Store.YES, Index.ANALYZED));

		NumericField numeric = new NumericField("long",
				Store.YES, true);
		numeric.setLongValue(low);
		first.add(numeric);

		second = new Document();
		second.add(new Field("Id", "second", Store.YES, Index.ANALYZED));

		numeric = new NumericField("long", Store.YES, true);
		numeric.setLongValue(mid);
		second.add(numeric);

		third = new Document();
		third.add(new Field("Id", "third", Store.YES, Index.ANALYZED));

		numeric = new NumericField("long", Store.YES, true);
		numeric.setLongValue(high);
		third.add(numeric);
		
		org.apache.lucene.index.IndexWriter writer =new org.apache.lucene.index.IndexWriter(new RAMDirectory(), new StandardAnalyzer(Version.LUCENE_CURRENT), MaxFieldLength.UNLIMITED);

		SimpleAnalyzer analyzer = new SimpleAnalyzer();

		writer.addDocument(first, analyzer);
		writer.addDocument(second, analyzer);
		writer.addDocument(third, analyzer);

		writer.commit();
		reader = writer.getReader();

	}


	@Test
	public void testLongRangeInclusive() throws Exception {

		NumericRangeQuery query = NumericRangeQuery.newLongRange("long",
				 mid, null, true, true);

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
				 mid, null, false, true);

		
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
				 null, mid, true, false);

		
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

		NumericRangeQuery query = NumericRangeQuery.newLongRange("long",
				 null, mid, true, true);

		
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
	public void testLongRangeZeroAll() throws Exception {

		// now we'll query from the middle inclusive

		NumericRangeQuery query = NumericRangeQuery.newLongRange("long",
				 Long.MIN_VALUE, null, true, true);

		
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
				 null, Long.MAX_VALUE, true, true);

	
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

