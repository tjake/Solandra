/**
 * 
 */
package lucandra;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.cassandra.db.marshal.BytesType;
import org.apache.commons.codec.binary.Hex;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.index.TermVectorMapper;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests byte ordering on string token values of trie packed data types
 * 
 * @author Todd Nine
 * 
 */
@Ignore("Ignored until this issue is fixed.  Numeric ranges won't work.  https://issues.apache.org/jira/browse/CASSANDRA-1235")
public class BytesOrderingEnumTest {

	private ByteComparator comparator = new ByteComparator();

	@Test
	public void simpleLongOrdering() throws Exception {

		// the first 2 shifts should make "first" larger. The shifts of 8 and 12
		// should
		// be identical values
		long low = 1277266160637L;
		long mid = low + 1000;
		long high = mid + 1000;

		List<byte[]> data = new ArrayList<byte[]>();

		NumericField numeric = new NumericField("long", Store.YES, true);
		numeric.setLongValue(low);

		setupByteList(numeric, data);

		numeric = new NumericField("long", Store.YES, true);
		numeric.setLongValue(mid);

		setupByteList(numeric, data);

		numeric = new NumericField("long", Store.YES, true);
		numeric.setLongValue(high);

		setupByteList(numeric, data);

		// first term of this query

		// iterate over all the terms
		StringBuffer key = new StringBuffer();
		// key.append('\u005c');
		// key.appen('\u0008');

		TestIndexReader reader = new TestIndexReader(data);
		
		IndexSearcher searcher = new IndexSearcher(reader);
		
		NumericRangeQuery query = NumericRangeQuery.newLongRange("long", 0L, null, true, true);

		
		TopDocs result = searcher.search(query, 1000);

		assertEquals(3, result.totalHits);

	}

	private void setupByteList(NumericField field, List<byte[]> data)
			throws Exception {
		TokenStream firstStream = field.tokenStreamValue();
		TermAttribute firstAttribute = (TermAttribute) firstStream
				.addAttribute(TermAttribute.class);

		ByteArrayOutputStream buffer = new ByteArrayOutputStream();

		byte[] bytes = null;
		int index = -1;

		// add all the tokens to our list
		while (firstStream.incrementToken()) {
			buffer.reset();
			buffer.write("longvals".getBytes("UTF-8"));
			buffer.write(CassandraUtils.delimeter.getBytes("UTF-8"));
			buffer.write("long".getBytes("UTF-8"));
			buffer.write(CassandraUtils.delimeter.getBytes("UTF-8"));
			buffer.write(firstAttribute.term().getBytes("UTF-8"));
			buffer.flush();
			bytes = buffer.toByteArray();

			index = Collections.binarySearch(data, bytes, comparator);

			if (index < 0) {
				index += 1;
				index *= -1;
			}

			data.add(index, bytes);
			
			System.out.println(String.format("bytes: %s.  Index : %s", String.valueOf(Hex.encodeHex(bytes)), index));
		}
	}

	/**
	 * The byte comparator for our arrays that uses the Cassandra
	 * 
	 * @author Todd Nine
	 * 
	 */
	private class ByteComparator implements Comparator<byte[]> {
		BytesType keyComparator = new BytesType();

		@Override
		public int compare(byte[] o1, byte[] o2) {
			return keyComparator.compare(o1, o2);
		}


	}
	
	private class TestIndexReader extends org.apache.lucene.index.IndexReader{

		private List<byte[]> data;

		public TestIndexReader(List<byte[]> data){
			this.data = data;
		}
		
		@Override
		protected void doClose() throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		protected void doCommit() throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		protected void doDelete(int docNum) throws CorruptIndexException,
				IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		protected void doSetNorm(int doc, String field, byte value)
				throws CorruptIndexException, IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		protected void doUndeleteAll() throws CorruptIndexException,
				IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public int docFreq(Term t) throws IOException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public Document document(int n, FieldSelector fieldSelector)
				throws CorruptIndexException, IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Collection getFieldNames(FieldOption fldOption) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public TermFreqVector getTermFreqVector(int docNumber, String field)
				throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void getTermFreqVector(int docNumber, TermVectorMapper mapper)
				throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void getTermFreqVector(int docNumber, String field,
				TermVectorMapper mapper) throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public TermFreqVector[] getTermFreqVectors(int docNumber)
				throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean hasDeletions() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean isDeleted(int n) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public int maxDoc() {
			//hard coded.  Our results should be 3
			return 4;
		}

		@Override
		public byte[] norms(String field) throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void norms(String field, byte[] bytes, int offset)
				throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public int numDocs() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public TermDocs termDocs() throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public TermPositions termPositions() throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public TermEnum terms() throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public TermEnum terms(Term t) throws IOException {
			
			ByteArrayOutputStream buffer = new ByteArrayOutputStream();

			buffer.write("longvals".getBytes("UTF-8"));
			buffer.write(CassandraUtils.delimeter.getBytes("UTF-8"));
			buffer.write("long".getBytes("UTF-8"));
			buffer.write(CassandraUtils.delimeter.getBytes("UTF-8"));
			buffer.write(t.text().getBytes("UTF-8"));
			buffer.flush();
			byte[]  bytes = buffer.toByteArray();
			
			ByteComparator comparator = new ByteComparator();
			
			int firstMatch = -1;
			
			for(int i =0; i < data.size(); i ++){
				if(comparator.compare(data.get(i), bytes) == 0){
					firstMatch = i;
					break;
				}
			}
			
			
			
			return new TestTermEnum(data, firstMatch, t.field());
			
		}
		
	}
	
	private class TestTermEnum extends TermEnum {
		
		
		private List<byte[]> data;
		private int startIndex;
		private String field;

		public TestTermEnum(List<byte[]> data, int startIndex, String field){
			this.data = data;
			this.startIndex = startIndex;
			this.field = field;
		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public int docFreq() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public boolean next() throws IOException {
			startIndex++;
			return startIndex < data.size();
		}

		@Override
		public Term term() {
			try {
				return new Term(field, new String( data.get(startIndex), "UTF-8"));
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
		}
		
		
	}
}
