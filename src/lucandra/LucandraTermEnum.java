package lucandra;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.cassandra.service.Cassandra;
import org.apache.cassandra.service.ColumnOrSuperColumn;
import org.apache.cassandra.service.ColumnParent;
import org.apache.cassandra.service.ConsistencyLevel;
import org.apache.cassandra.service.InvalidRequestException;
import org.apache.cassandra.service.SlicePredicate;
import org.apache.cassandra.service.SliceRange;
import org.apache.cassandra.service.UnavailableException;
import org.apache.log4j.Logger;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.thrift.TException;

/**
 * 
 * @author jake
 * 
 */
public class LucandraTermEnum extends TermEnum {

    private final IndexReader indexReader;
    private final String indexName;

    private int termPosition;
    private Term[] termBuffer;
    private TreeMap<Term, List<ColumnOrSuperColumn>> termDocFreqBuffer;

    private Map<String, TreeMap<Term, List<ColumnOrSuperColumn>>> termCache;

    private final Cassandra.Client client;

    private static final Logger logger = Logger.getLogger(LucandraTermEnum.class);

    public LucandraTermEnum(IndexReader indexReader) {
        this.indexReader = indexReader;
        this.indexName = indexReader.getIndexName();
        this.client = indexReader.getClient();
        this.termPosition = 0;

        this.termCache = new HashMap<String, TreeMap<Term, List<ColumnOrSuperColumn>>>();
    }

    @Override
    public boolean skipTo(Term term) throws IOException {
        loadTerms(term);

        return termBuffer.length == 0 ? false : true;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public int docFreq() {
        return termDocFreqBuffer.size();
    }

    @Override
    public boolean next() throws IOException {

        termPosition++;

        boolean hasNext = termPosition < termBuffer.length;

        if (!hasNext)
            termPosition = 0;

        return hasNext;
    }

    @Override
    public Term term() {
        return termBuffer[termPosition];
    }

    private void loadTerms(Term skipTo) {

        // chose starting term
        String startTerm = indexName + "/" + CassandraUtils.createColumnName(skipTo);
        // this is where we stop;
        String endTerm = startTerm + new Character((char) 255);

        termDocFreqBuffer = termCache.get(startTerm);

        if (termDocFreqBuffer != null) {

            termBuffer = termDocFreqBuffer.keySet().toArray(new Term[] {});
            termPosition = 0;

            logger.info("Found " + startTerm + " in cache");
            return;
        }

        long start = System.currentTimeMillis();

        // First buffer the keys in this term range
        List<String> keys;
        try {
            keys = client.get_key_range(CassandraUtils.keySpace, CassandraUtils.termVecColumnFamily, startTerm, endTerm, Integer.MAX_VALUE,
                    ConsistencyLevel.ONE);
        } catch (InvalidRequestException e) {
            throw new RuntimeException(e);
        } catch (TException e) {
            throw new RuntimeException(e);
        } catch (UnavailableException e) {
            throw new RuntimeException(e);
        }

        logger.info("Found " + keys.size() + " keys in range:" + startTerm + " to " + endTerm);

        termDocFreqBuffer = new TreeMap<Term, List<ColumnOrSuperColumn>>();

        if (!keys.isEmpty()) {
            ColumnParent columnParent = new ColumnParent(CassandraUtils.termVecColumnFamily, null);
            SlicePredicate slicePredicate = new SlicePredicate();

            // Get all columns
            SliceRange sliceRange = new SliceRange(new byte[] {}, new byte[] {}, false, Integer.MAX_VALUE);
            slicePredicate.setSlice_range(sliceRange);

            Map<String, List<ColumnOrSuperColumn>> columns;

            try {
                columns = client.multiget_slice(CassandraUtils.keySpace, keys, columnParent, slicePredicate, ConsistencyLevel.ONE);
            } catch (InvalidRequestException e) {
                throw new RuntimeException(e);
            } catch (TException e) {
                throw new RuntimeException(e);
            } catch (UnavailableException e) {
                throw new RuntimeException(e);
            }

            for (Map.Entry<String, List<ColumnOrSuperColumn>> entry : columns.entrySet()) {

                String termStr = entry.getKey().split("/")[1];
                Term term = CassandraUtils.parseTerm(termStr.getBytes());

                termDocFreqBuffer.put(term, entry.getValue());
            }
        }

        // put in cache
        termCache.put(startTerm, termDocFreqBuffer);

        termBuffer = termDocFreqBuffer.keySet().toArray(new Term[] {});
        termPosition = 0;

        long end = System.currentTimeMillis();

        logger.info("loadTerms: " + startTerm + "(" + termBuffer.length + ") took " + (end - start) + "ms");

    }

    public final List<ColumnOrSuperColumn> getTermDocFreq() {
        if(termBuffer.length == 0)
            return null;
        
        List<ColumnOrSuperColumn> termDocs = termDocFreqBuffer.get(termBuffer[termPosition]);

        // reverse time ordering
        Collections.reverse(termDocs);

        return termDocs;
    }

}
