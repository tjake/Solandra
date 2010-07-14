package lucandra;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.SliceByNamesReadCommand;
import org.apache.cassandra.db.SliceFromReadCommand;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermVectorOffsetInfo;

public class TermFreqVector implements org.apache.lucene.index.TermFreqVector, org.apache.lucene.index.TermPositionVector {

    private String field;
    private byte[] docId;
    private String[] terms;
    private int[] freqVec;
    private int[][] termPositions;
    private TermVectorOffsetInfo[][] termOffsets;

    public TermFreqVector(String indexName, String field, byte[] docId) {
        this.field = field;
        this.docId = docId;

        byte[] key = CassandraUtils.hashKeyBytes(indexName.getBytes(), CassandraUtils.delimeterBytes,  docId);

        ReadCommand rc = new SliceByNamesReadCommand(CassandraUtils.keySpace, key, CassandraUtils.metaColumnPath, Arrays
                .asList(CassandraUtils.documentMetaFieldBytes));

        List<Row> rows = null;
        int attempts = 0;
        while (attempts++ < 10) {
            try {
                rows = StorageProxy.readProtocol(Arrays.asList(rc), ConsistencyLevel.ONE);
                break;
            } catch (IOException e1) {
                throw new RuntimeException(e1);
            } catch (UnavailableException e1) {

            } catch (TimeoutException e1) {

            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {

            }
        }

        if (attempts >= 10)
            throw new RuntimeException("Read command failed after 10 attempts");

        if (rows.isEmpty())
            return; // nothing to delete

        List<Term> allTerms;
        try {
            allTerms = (List<Term>) CassandraUtils.fromBytes(rows.get(0).cf.getColumn(CassandraUtils.documentMetaFieldBytes).value());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        List<ReadCommand> readCommands = new ArrayList<ReadCommand>();

        for (Term t : allTerms) {

            // skip the ones not of this field
            if (!t.field().equals(field))
                continue;

            // add to multiget params
            try {
                key = CassandraUtils.hashKeyBytes(indexName.getBytes(),  CassandraUtils.delimeterBytes, t.field().getBytes(), CassandraUtils.delimeterBytes, t.text().getBytes("UTF-8"));
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("JVM doesn't support UTF-8",e);
            }

            readCommands.add(new SliceFromReadCommand(CassandraUtils.keySpace, key, new ColumnParent().setColumn_family(CassandraUtils.termVecColumnFamily)
                    .setSuper_column(docId), new byte[] {}, new byte[] {}, false, 1024));
        }

        try {
            rows = StorageProxy.readProtocol(readCommands, ConsistencyLevel.ONE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (UnavailableException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }

        terms = new String[rows.size()];
        freqVec = new int[rows.size()];
        termPositions = new int[rows.size()][];
        termOffsets = new TermVectorOffsetInfo[rows.size()][];

        int i = 0;

        for (Row row : rows) {
            String rowKey;
            try {
                rowKey = new String(row.key.key,"UTF-8");
            } catch (UnsupportedEncodingException e) {
               throw new RuntimeException("JVM does not support UTF-8");
            }
            String termStr = rowKey.substring(rowKey.indexOf(CassandraUtils.delimeter) + CassandraUtils.delimeter.length());

            Term t = CassandraUtils.parseTerm(termStr);

            terms[i] = t.text();

            // Find the offsets and positions
            IColumn positionVector = row.cf.getSortedColumns().iterator().next().getSubColumn(CassandraUtils.positionVectorKey.getBytes());
            IColumn offsetVector = row.cf.getSortedColumns().iterator().next().getSubColumn(CassandraUtils.offsetVectorKey.getBytes());

            termPositions[i] = positionVector == null ? new int[] {} : CassandraUtils.byteArrayToIntArray(positionVector.value());
            freqVec[i] = termPositions[i].length;

            if (offsetVector == null) {
                termOffsets[i] = TermVectorOffsetInfo.EMPTY_OFFSET_INFO;
            } else {

                int[] offsets = CassandraUtils.byteArrayToIntArray(offsetVector.value());

                termOffsets[i] = new TermVectorOffsetInfo[freqVec[i]];
                for (int j = 0, k = 0; j < offsets.length; j += 2, k++) {
                    termOffsets[i][k] = new TermVectorOffsetInfo(offsets[j], offsets[j + 1]);
                }
            }

            i++;
        }

    }

    public String getField() {
        return field;
    }

    public int[] getTermFrequencies() {
        return freqVec;
    }

    public String[] getTerms() {
        return terms;
    }

    public int indexOf(String term) {
        return Arrays.binarySearch(terms, term);
    }

    public int[] indexesOf(String[] terms, int start, int len) {
        int[] res = new int[terms.length];

        for (int i = 0; i < terms.length; i++) {
            res[i] = indexOf(terms[i]);
        }

        return res;
    }

    public int size() {
        return terms.length;
    }

    public TermVectorOffsetInfo[] getOffsets(int index) {
        return termOffsets[index];
    }

    public int[] getTermPositions(int index) {
        return termPositions[index];
    }

}
