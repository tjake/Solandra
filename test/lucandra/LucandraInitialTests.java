package lucandra;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.cassandra.thrift.Cassandra.Iface;
import org.apache.log4j.BasicConfigurator;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Version;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LucandraInitialTests {

	private Iface conn;
	private IndexWriter indexWriter;
	private Analyzer analyzer;
	private IndexReader indexReader;

	@Before
	public void setup() throws TTransportException {
		BasicConfigurator.configure();

		conn = CassandraUtils.createConnection("localhost", Integer.valueOf(9160), false);
		String testIndex = "testIndex";
		indexWriter = new IndexWriter(testIndex, conn);

		analyzer = new WhitespaceAnalyzer();

		indexReader = new IndexReader(testIndex, conn);
	}

	@Test
	public void shouldInsertAndRetrieveDocument() throws Exception {
		QueryParser qp = new QueryParser(Version.LUCENE_29, "key", analyzer);

		indexWriter.deleteDocuments(qp.parse("valore"));

		indexWriter.commit();

		Document doc1 = new Document();
		doc1.add(new Field("key", "valore della chiave", Field.Store.YES, Field.Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
		doc1.add(new Field("value", "valore del valore", Field.Store.YES, Field.Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));

		indexWriter.addDocument(doc1, analyzer);

		System.out.println(indexWriter.docCount());

		indexWriter.commit();

		indexReader.reopen();

		IndexSearcher indexSearcher = new IndexSearcher(indexReader);

		TopDocs search = indexSearcher.search(qp.parse("valore"), 100);
		assertEquals(1, search.totalHits);

		indexWriter.deleteDocuments(qp.parse("valore"));

		indexWriter.commit();

	}

	@Test
	public void shouldInsertAndRetrieveDocuments() throws Exception {
		QueryParser qp = new QueryParser(Version.LUCENE_29, "key", analyzer);

		indexWriter.deleteDocuments(qp.parse("chiave"));

		indexWriter.commit();

		long start = System.currentTimeMillis();

		int docs = 1000;
		for (int i = 0; i < docs; i++) {
			Document doc1 = new Document();
			doc1.add(new Field("key", "chiave " + i, Field.Store.YES, Field.Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
			doc1.add(new Field("value", "valore " + i, Field.Store.YES, Field.Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
			indexWriter.addDocument(doc1, analyzer);

		}

		System.out.println("docCount:: " + indexWriter.docCount());

		indexWriter.commit();

		long stop = System.currentTimeMillis();

		System.out.println("time:: " + (stop - start) / 1000);
		indexReader.reopen();

		IndexSearcher indexSearcher = new IndexSearcher(indexReader);

		TopDocs search = indexSearcher.search(qp.parse("chiave"), docs);
		assertEquals(docs, search.totalHits);

		indexWriter.deleteDocuments(qp.parse("chiave"));

		indexWriter.commit();

	}

	@Test
	public void shouldInsertAndRetrieveDocumentsFromMultipleIndexes() throws Exception {

		cleanIndex("testIndex1");
		cleanIndex("testIndex2");
		cleanIndex("testIndex3");
	
		long start = System.currentTimeMillis();

		int docs = 1000;
		loadIndex("testIndex1", conn, docs);
		loadIndex("testIndex2", conn, docs);
		loadIndex("testIndex3", conn, docs);

		System.out.println("docCount:: " + indexWriter.docCount());

		long stop = System.currentTimeMillis();

		System.out.println("time:: " + (stop - start) / 1000);
		MultiReader reader = new MultiReader(asList(new IndexReader("testIndex1", conn), new IndexReader("testIndex2", conn), new IndexReader("testIndex3", conn)).toArray(new IndexReader[] {}));

		IndexSearcher indexSearcher = new IndexSearcher(reader);

		QueryParser qp = new QueryParser(Version.LUCENE_29, "key", analyzer);

		TopDocs search = indexSearcher.search(qp.parse("chiave"), docs);
		
		reader.numDocs();
		assertEquals(docs * 3, search.totalHits);

		cleanIndex("testIndex1");
		cleanIndex("testIndex2");
		cleanIndex("testIndex3");

	}

	private void loadIndex(String index, Iface conn, int docNumber) throws Exception {

		IndexWriter indexWriter = new IndexWriter(index, conn);
		System.out.println("adding to :: " + index);

		for (int i = 0; i < docNumber; i++) {
			Document doc1 = new Document();
			doc1.add(new Field("key", "chiave " + i, Field.Store.YES, Field.Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
			doc1.add(new Field("value", "valore " + i, Field.Store.YES, Field.Index.ANALYZED, TermVector.WITH_POSITIONS_OFFSETS));
			indexWriter.addDocument(doc1, analyzer);
			System.out.print(".");
		}
		System.out.println("done");

		indexWriter.commit();

	}

	private void cleanIndex(String index) throws CorruptIndexException, IOException, ParseException {
		QueryParser qp = new QueryParser(Version.LUCENE_29, "key", analyzer);

		IndexWriter indexWriter = new IndexWriter(index, conn);

		indexWriter.deleteDocuments(qp.parse("chiave"));

		indexWriter.commit();
	}

	@After
	public void teardown() throws Exception {
		indexReader.close();

		BasicConfigurator.resetConfiguration();
	}
}
