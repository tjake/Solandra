package lucandra;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.SliceFromReadCommand;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.log4j.Logger;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;

public class LucandraAllTermDocs implements TermDocs
{

    private static Logger logger    = Logger.getLogger(LucandraAllTermDocs.class);
    private String        indexName;
    private int           idx;                                                    // tracks
    // how
    // where
    // we
    // are
    // in
    // the
    // doc
    // buffer
    private int           fillSize;                                               // tracks
    // how
    // much
    // the
    // buffer
    // was
    // filled
    // with
    // docs
    // from
    // cassandra
    private int[]         docBuffer = new int[128];                               // max
    // number
    // of
    // docs
    // we
    // pull
    private int           doc       = -1;
    private int           maxDoc;

    public LucandraAllTermDocs(IndexReader indexReader)
    {
        indexName = indexReader.getIndexName();
        maxDoc = indexReader.maxDoc();
        idx = 0;
        fillSize = 0;
    }

    public void seek(Term term) throws IOException
    {
        if (term == null)
        {
            doc = -1;
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }

    public void seek(TermEnum termEnum) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public int doc()
    {
        return doc;
    }

    public int freq()
    {
        return 1;
    }

    public boolean next() throws IOException
    {
        return skipTo(doc + 1);
    }

    public int read(int[] docs, int[] freqs) throws IOException
    {
        final int length = docs.length;
        int i = 0;
        while (i < length && doc < maxDoc && fillSize > 0)
        {

            docs[i] = doc;
            freqs[i] = 1;
            ++i;

            next();
        }
        return i;
    }

    public boolean skipTo(int target) throws IOException
    {
        doc = target;

        do
        {
            if (idx >= fillSize)
                getMoreDocs();

            for (; idx < fillSize; idx++)
            {
                if (docBuffer[idx] >= doc)
                    return true;
            }

        }
        while (doc < maxDoc && fillSize > 0);

        return false;
    }

    public void close() throws IOException
    {
    }

    private void getMoreDocs()
    {
        List<ReadCommand> readCommands = new ArrayList<ReadCommand>();

        ColumnParent columnParent = new ColumnParent();
        columnParent.setColumn_family(CassandraUtils.docColumnFamily);

        idx = 0;
        fillSize = 0;
        logger.info("Getting more docs for " + indexName);

        do
        {

            readCommands.clear();

            for (int i = doc; i < (doc + docBuffer.length) && i < maxDoc; i++)
            {
                String docHex = Integer.toHexString(i);
                logger.debug("Scanning index " + indexName + " " + i);

                try
                {
                    ByteBuffer key = CassandraUtils.hashKeyBytes(indexName.getBytes(), CassandraUtils.delimeterBytes,
                            docHex.getBytes("UTF-8"));

                    readCommands.add(new SliceFromReadCommand(CassandraUtils.keySpace, key, columnParent,
                            FBUtilities.EMPTY_BYTE_BUFFER, CassandraUtils.finalTokenBytes, false, Integer.MAX_VALUE));
                }
                catch (UnsupportedEncodingException e)
                {
                    throw new RuntimeException(e);
                }
            }

            List<Row> rows = CassandraUtils.robustGet(readCommands, ConsistencyLevel.ONE);

            // No more docs!
            if (rows.isEmpty())
                return;

            int numNulls = 0;

            for (Row row : rows)
            {

                if (row.cf == null || row.cf.isMarkedForDelete())
                {
                    numNulls++;
                    continue;
                }

                String key = ByteBufferUtil.string(row.key.key, CassandraUtils.UTF_8);

                // term keys look like indexName/docNum
                String docHex = key
                        .substring(key.indexOf(CassandraUtils.delimeter) + CassandraUtils.delimeter.length());

                Integer docNum = Integer.valueOf(docHex, 16);
                docBuffer[fillSize] = docNum;
                fillSize++;
            }

            if (fillSize == 0 && numNulls >= readCommands.size())
                return;

        }
        while (fillSize == 0);
    }

}
