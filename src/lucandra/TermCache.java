package lucandra;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.cassandra.db.*;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.log4j.Logger;
import org.apache.lucene.index.Term;

public class TermCache
{
    
    private final static Term emptyTerm = new Term("");
    private final static ConcurrentNavigableMap<Term, LucandraTermInfo[]> emptyMap = new ConcurrentSkipListMap<Term, LucandraTermInfo[]>();
    private final static ColumnParent            fieldColumnFamily = new ColumnParent(CassandraUtils.metaInfoColumnFamily);
    private final static Logger                  logger = Logger.getLogger(TermCache.class);
    
    public final String                                               indexName;
    public final ByteBuffer                                           termsListKey;
    public final ConcurrentSkipListMap<Term, LucandraTermInfo[]>      termList;
    public final ConcurrentSkipListMap<Term, Pair<Term,Term>>         termQueryBoundries;
  

    public TermCache(String indexName) throws IOException
    {
        this.indexName = indexName;
        termsListKey   = CassandraUtils.hashKeyBytes(indexName.getBytes("UTF-8"), CassandraUtils.delimeterBytes, "terms".getBytes("UTF-8"));        
        termList       = new ConcurrentSkipListMap<Term, LucandraTermInfo[]>();

        //Get the boundries of terms each term
        termQueryBoundries    = new ConcurrentSkipListMap<Term, Pair<Term,Term>>();        
    }
    
    //Cache check only
    public LucandraTermInfo[] get(Term term)
    {
        return termList.get(term);      
    }
    
    public ConcurrentNavigableMap<Term, LucandraTermInfo[]> skipTo(Term skip) throws IOException
    {
        
        Pair<Term,Term> range = null;
        
        int bufferSize = termList.isEmpty() ? 4 : 64;
           
        //verify we've buffered sufficiently        
        Map.Entry<Term, Pair<Term,Term>> tailEntry = termQueryBoundries.ceilingEntry(skip);
        boolean needsBuffering = true;
         
        if(tailEntry != null)
        {
            range = tailEntry.getValue();
            
            if(skip.compareTo(range.left) >= 0 && (!range.right.equals(emptyTerm) && skip.compareTo(range.right) < 0))
                needsBuffering = false;                      
        }
        
        ConcurrentNavigableMap<Term, LucandraTermInfo[]> subList = emptyMap;
        
        if(needsBuffering)
        {
            range = bufferTerms(skip, bufferSize);    
        }
        
        if(skip.compareTo(range.left) >= 0 && (!range.right.equals(emptyTerm)) && skip.compareTo(range.right) <= 0)
        {
            subList = termList.subMap(skip, true, range.right, true);            
        }
        
        return subList;
    }
    
    
    public  static LucandraTermInfo[] convertTermInfo(Collection<IColumn> docs)
    {

        LucandraTermInfo termInfo[] = new LucandraTermInfo[docs.size()];

        int i = 0;
        for (IColumn col : docs)
        {
            if (i == termInfo.length)
                break;

            if (i == 0 && col instanceof SuperColumn)
                throw new IllegalStateException(
                        "TermInfo ColumnFamily is a of type Super: This is no longer supported, please see NEWS.txt");

            if (col == null || col.name() == null || col.value() == null)
                throw new IllegalStateException("Encountered missing column: " + col);

            termInfo[i] = new LucandraTermInfo(CassandraUtils.readVInt(col.name()), col.value());
            i++;
        }
        
        return termInfo;
    }
    
    public Pair<Term,Term> bufferTerms(Term startTerm, int bufferSize) throws IOException
    {
        assert bufferSize > 0;
        
        long start = System.currentTimeMillis();
              
        // Scan range of terms in this field (reversed, so we have a exit point)
        List<Row> rows = CassandraUtils.robustRead(CassandraUtils.consistency,
                new SliceFromReadCommand(CassandraUtils.keySpace, termsListKey, fieldColumnFamily, CassandraUtils.createColumnName(startTerm),
                        ByteBufferUtil.EMPTY_BYTE_BUFFER, false, bufferSize));

        ColumnParent columnParent = new ColumnParent(CassandraUtils.termVecColumnFamily);

        
        // Collect read commands
        Collection<IColumn> columns;
        
        if (rows == null || rows.size() != 1 || rows.get(0).cf == null)
        {
            columns = new ArrayList<IColumn>();
        }
        else 
        {
            columns = rows.get(0).cf.getSortedColumns();
            
            if(logger.isDebugEnabled())
                logger.debug("Found "+columns.size()+" terms under field "+startTerm.field());
        }
        

        Term endTerm;
        Pair<Term,Term> queryRange;
                
        if(!columns.isEmpty())
        {
            //Yuck, need last column
            IColumn lastColumn = null;
            Iterator<IColumn> it = columns.iterator();
            
            while(it.hasNext())
                lastColumn = it.next();
            
            endTerm = CassandraUtils.parseTerm(ByteBufferUtil.string(lastColumn.name(),CassandraUtils.UTF_8));
            queryRange = new Pair<Term, Term>(startTerm, endTerm);
        }
        else
        {
            queryRange = new Pair<Term, Term>(startTerm, emptyTerm);
            
            termQueryBoundries.put(startTerm, queryRange);
            
            return queryRange;
        }
        
        
        Map<Term,Pair<Term,Term>> localRanges = new HashMap<Term,Pair<Term,Term>>(columns.size());
        localRanges.put(startTerm, queryRange);
              
        List<ReadCommand> reads = new ArrayList<ReadCommand>(columns.size());
        for (IColumn column : columns)
        {           
            Term term = CassandraUtils.parseTerm(ByteBufferUtil.string(column.name(),CassandraUtils.UTF_8));
            
            
            localRanges.put(term, queryRange);
            
            ByteBuffer rowKey;
            try
            {
                rowKey = CassandraUtils.hashKeyBytes(indexName.getBytes("UTF-8"),    CassandraUtils.delimeterBytes, 
                                                                term.field().getBytes("UTF-8"), CassandraUtils.delimeterBytes, 
                                                                term.text().getBytes("UTF-8"));
            }
            catch (UnsupportedEncodingException e)
            {
                throw new RuntimeException("This JVM doesn't support UTF-8");
            }

            if (logger.isDebugEnabled())
                logger.debug("scanning row: " + ByteBufferUtil.string(rowKey));
            
            
            reads.add((ReadCommand) new SliceFromReadCommand(CassandraUtils.keySpace, rowKey, columnParent,
                    ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, Integer.MAX_VALUE));
        }

        rows = CassandraUtils.robustRead(CassandraUtils.consistency, reads.toArray(new ReadCommand[] {}));

        // term to start with next time
        int actualReadSize = rows.size();
        
        if (logger.isDebugEnabled())
        {
            logger.debug("Found " + rows.size() + " rows in range:" + startTerm + " to "
                    + "" + " in "
                    + (System.currentTimeMillis() - start) + "ms");

        }

        if (actualReadSize > 0)
        {
            for (Row row : rows)
            {

                if (row.cf == null)
                    continue;

                String key = ByteBufferUtil.string(row.key.key, CassandraUtils.UTF_8);
               
                // term keys look like wikipedia/body/wiki
                String termStr = key.substring(key.indexOf(CassandraUtils.delimeter) + CassandraUtils.delimeter.length());
                Term term = CassandraUtils.parseTerm(termStr);

                columns = row.cf.getSortedColumns();

                if(logger.isDebugEnabled())
                    logger.debug(term + " has " + columns.size());
           
                // remove any deleted columns
                Collection<IColumn> columnsToRemove = null;

                for (IColumn col : columns)
                {
                    if (!col.isLive())
                    {
                        if (columnsToRemove == null)
                            columnsToRemove = new ArrayList<IColumn>();
                                
                        columnsToRemove.add(col);
                    }
                            
                    if(logger.isDebugEnabled())
                        logger.debug("Kept DocId "+CassandraUtils.readVInt(col.name()));
                }

                if (columnsToRemove != null)
                {
                    columns.removeAll(columnsToRemove);
                }
                        
                if (!columns.isEmpty())
                {
                    if (logger.isDebugEnabled())
                        logger.debug("saving term: " + term + " with "+columns.size()+" docs");

                    termList.put(term, convertTermInfo(columns));
                }
                else
                {
                    if(logger.isDebugEnabled())
                        logger.debug("Skipped term: "+term);
                } 
            }
            
            //to recall we did this query
            termQueryBoundries.putAll(localRanges);           
        }

        long end = System.currentTimeMillis();

        if (logger.isDebugEnabled())
        {
            logger.debug("loadTerms: " + startTerm + "(" + actualReadSize + ") took "+ (end - start) + "ms");
        }
               
        return queryRange;
    }  
}
