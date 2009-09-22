package lucandra;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.cassandra.service.Cassandra;
import org.apache.cassandra.service.Column;
import org.apache.cassandra.service.ColumnOrSuperColumn;
import org.apache.cassandra.service.ColumnParent;
import org.apache.cassandra.service.ConsistencyLevel;
import org.apache.cassandra.service.InvalidRequestException;
import org.apache.cassandra.service.NotFoundException;
import org.apache.cassandra.service.SlicePredicate;
import org.apache.cassandra.service.SliceRange;
import org.apache.cassandra.service.SuperColumn;
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
    private TreeMap<Term, List<Column>> termDocFreqBuffer;

    private Map<String, TreeMap<Term, List<Column>>> termCache;

    private final Cassandra.Client client;

    private static final Logger logger = Logger.getLogger(LucandraTermEnum.class);

    public LucandraTermEnum(IndexReader indexReader) {
        this.indexReader = indexReader;
        this.indexName = indexReader.getIndexName();
        this.client = indexReader.getClient();
        this.termPosition = 0;

        this.termCache = new HashMap<String, TreeMap<Term, List<Column>>>();
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
        String startTerm = CassandraUtils.createColumnName(skipTo);

        // this is where we stop;
        String endTerm = startTerm + "zzzzzzzzzzzzzzzzzzzzzzz";

        termDocFreqBuffer = termCache.get(startTerm);

        if (termDocFreqBuffer != null) {

            termBuffer = termDocFreqBuffer.keySet().toArray(new Term[] {});
            termPosition = 0;

            logger.info("Found " + startTerm + " in cache");
            return;
        }

        ColumnParent columnParent = new ColumnParent();
        columnParent.setColumn_family(CassandraUtils.termVecColumn);

        // create predicate
        SlicePredicate slicePredicate = new SlicePredicate();
        SliceRange sliceRange = new SliceRange();
        slicePredicate.setSlice_range(sliceRange);

        sliceRange.setStart(startTerm.getBytes());
        sliceRange.setFinish(endTerm.getBytes());
        sliceRange.setCount(Integer.MAX_VALUE);

        List<ColumnOrSuperColumn> termColumns;

        long start = System.currentTimeMillis();

        try {

            termColumns = client.get_slice(CassandraUtils.keySpace, indexName, columnParent, slicePredicate, ConsistencyLevel.ONE);

        } catch (InvalidRequestException e) {
            throw new RuntimeException(e);
        } catch (NotFoundException e) {
            throw new RuntimeException(e);
        } catch (TException e) {
            throw new RuntimeException(e);
        } catch (UnavailableException e) {
            throw new RuntimeException(e);
        }

        termDocFreqBuffer = new TreeMap<Term, List<Column>>();

        // parse results
        for (ColumnOrSuperColumn termColumn : termColumns) {
            SuperColumn termSuperColumn = termColumn.getSuper_column();

            try {
                if (endTerm.compareToIgnoreCase(new String(termSuperColumn.getName(), "UTF-8")) < 0) {
                    break;
                }
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }

            termDocFreqBuffer.put(CassandraUtils.parseTerm(termSuperColumn.getName()), termSuperColumn.getColumns());
        }

        // put in cache
        termCache.put(startTerm, termDocFreqBuffer);

        termBuffer = termDocFreqBuffer.keySet().toArray(new Term[] {});
        termPosition = 0;

        long end = System.currentTimeMillis();

        logger.info("loadTerms: " + startTerm + "(" + termBuffer.length + ") took " + (end - start) + "ms");

    }

    public final List<Column> getTermDocFreq() {
        List<Column> termDocs = termDocFreqBuffer.get(termBuffer[termPosition]);

        // reverse time ordering
        Collections.reverse(termDocs);

        return termDocs;
    }

}
