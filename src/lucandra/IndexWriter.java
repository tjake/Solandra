package lucandra;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.service.Cassandra;
import org.apache.cassandra.service.Column;
import org.apache.cassandra.service.ColumnOrSuperColumn;
import org.apache.cassandra.service.ColumnPath;
import org.apache.cassandra.service.ConsistencyLevel;
import org.apache.cassandra.service.InvalidRequestException;
import org.apache.cassandra.service.UnavailableException;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.thrift.TException;

public class IndexWriter {

    private final String indexName;
    private final Cassandra.Client client;

    private static final Logger logger = Logger.getLogger(IndexWriter.class);

    public IndexWriter(String indexName, Cassandra.Client client) {

        this.indexName = indexName;
        this.client = client;

    }

    public void addDocument(Document doc, Analyzer analyzer) throws CorruptIndexException, IOException {

        Token token = new Token();

        // Build wacky batch struct
        Map<String, List<ColumnOrSuperColumn>> cfMap = new HashMap<String, List<ColumnOrSuperColumn>>();

        // FIXME: This should accept
        String docId = Long.toHexString(System.nanoTime());

        ColumnPath termVecColumnPath = new ColumnPath(CassandraUtils.termVecColumnFamily, null, docId.getBytes());
        
        
        int position = 0;

        for (Field field : (List<Field>) doc.getFields()) {

            // Indexed field
            if (field.isIndexed() && field.isTokenized()) {

                TokenStream tokens = field.tokenStreamValue();

                if (tokens == null) {
                    tokens = analyzer.tokenStream(field.name(), new StringReader(field.stringValue()));
                }

                // collect term frequencies per doc
                Map<String, List<Integer>> termPositions = new HashMap<String, List<Integer>>();
                int lastOffset = 0;
                if (position > 0) {
                    position += analyzer.getPositionIncrementGap(field.name());
                }

                // Build the termPositions vector for all terms
                while (tokens.next(token) != null) {
                    String term = CassandraUtils.createColumnName(field.name(), token.term());

                    List<Integer> pvec = termPositions.get(term);

                    if (pvec == null) {
                        pvec = new ArrayList<Integer>();
                        termPositions.put(term, pvec);
                    }

                    position += (token.getPositionIncrement() - 1);
                    pvec.add(++position);

                }

                for (Map.Entry<String, List<Integer>> term : termPositions.entrySet()) {

                    // Terms are stored within a unique key combination
                    // This is required since cassandra loads all column
                    // families for a key into memory
                    String key = indexName + "/" + term.getKey();

                    CassandraUtils.robustInsert(client, key, termVecColumnPath, CassandraUtils.intVectorToByteArray(term.getValue()));
                }
            }

            //Untokenized fields go in without a termPosition
            if (field.isIndexed() && !field.isTokenized()) {
                String term = CassandraUtils.createColumnName(field.name(), field.stringValue());

                String key = indexName + "/" + term;

                CassandraUtils.robustInsert(client, key, termVecColumnPath, CassandraUtils.intVectorToByteArray(Arrays.asList(new Integer[] { 0 })));
            }

            // Stores each field as a column under this doc key
            if (field.isStored()) {

                byte[] value = field.isBinary() ? field.getBinaryValue() : field.stringValue().getBytes();

                ColumnPath docColumnPath = new ColumnPath(CassandraUtils.docColumnFamily, null, field.name().getBytes());

                CassandraUtils.robustInsert(client, indexName+"/"+docId, docColumnPath, value);
            }
        }
    }

    public void deleteDocuments(Query query) throws CorruptIndexException, IOException {
        throw new UnsupportedOperationException();
    }

    public void deleteDocuments(Term arg0) throws CorruptIndexException, IOException {
        throw new UnsupportedOperationException();
    }

    public int docCount() {

        try {
            String start = indexName + "/";
            String finish = indexName + new Character((char) 255);

            return client.get_key_range(CassandraUtils.keySpace, CassandraUtils.docColumnFamily, start, finish, Integer.MAX_VALUE, ConsistencyLevel.ONE).size();
        } catch (TException e) {
            throw new RuntimeException(e);
        } catch (InvalidRequestException e) {
            throw new RuntimeException(e);
        } catch (UnavailableException e) {
            throw new RuntimeException(e);
        }

    }

}
