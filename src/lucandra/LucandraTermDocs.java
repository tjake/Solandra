package lucandra;

import java.io.IOException;
import java.util.List;

import org.apache.cassandra.service.Column;
import org.apache.log4j.Logger;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermPositions;

public class LucandraTermDocs implements TermDocs, TermPositions {

    private IndexReader indexReader;
    private LucandraTermEnum termEnum;
    private List<Column> termDocs;
    private int docPosition;
    private int[] termPositionArray;
    private int termPosition;
    private static final Logger logger = Logger.getLogger(LucandraTermDocs.class);

    public LucandraTermDocs(IndexReader indexReader) {
        this.indexReader = indexReader;
        termEnum = new LucandraTermEnum(indexReader);
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public int doc() {
        if (docPosition < 0)
            docPosition = 0;

        int docid = indexReader.addDocument(termDocs.get(docPosition).getName());

        return docid;
    }

    @Override
    public int freq() {

        termPositionArray = CassandraUtils.byteArrayToIntArray(termDocs.get(docPosition).getValue());
        termPosition = 0;

        return termPositionArray.length;
    }

    @Override
    public boolean next() throws IOException {

        if (termDocs == null)
            return false;

        return ++docPosition < termDocs.size();
    }

    @Override
    public int read(int[] docs, int[] freqs) throws IOException {

        int i = 0;
        for (; (termDocs != null && docPosition < termDocs.size() && i < docs.length); i++, docPosition++) {
            docs[i] = doc();
            freqs[i] = freq();
        }

        return i;
    }

    @Override
    public void seek(Term term) throws IOException {
        // on a new term so check cached
        LucandraTermEnum tmp = indexReader.checkTermCache(term);
        if (tmp == null) {

            if (termEnum.skipTo(term)) {
                if (termEnum.term().compareTo(term) == 0) {
                    termDocs = termEnum.getTermDocFreq();
                } else {
                    termDocs = null;
                }
            }
        } else {
            termEnum = tmp;
            termEnum.skipTo(term);
            termDocs = termEnum.getTermDocFreq();
        }

        docPosition = -1;
    }

    @Override
    public void seek(TermEnum termEnum) throws IOException {
        if (termEnum instanceof LucandraTermEnum) {
            this.termEnum = (LucandraTermEnum) termEnum;
            termDocs = this.termEnum.getTermDocFreq();
            docPosition = -1;
        } else {
            throw new RuntimeException("TermEnum is not compatable with Lucandra");
        }
    }

    @Override
    public boolean skipTo(int target) throws IOException {
        do {
            if (!next())
                return false;
        } while (target > doc());

        return true;
    }

    @Override
    public byte[] getPayload(byte[] data, int offset) throws IOException {
        return null;
    }

    @Override
    public int getPayloadLength() {
        return 0;
    }

    @Override
    public boolean isPayloadAvailable() {
        return false;
    }

    @Override
    public int nextPosition() throws IOException {
        int pos = termPositionArray[termPosition];
        termPosition++;

        return pos;
    }

}
