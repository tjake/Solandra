package lucandra;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermVectorOffsetInfo;
import org.apache.thrift.TException;

public class TermFreqVector implements org.apache.lucene.index.TermFreqVector, org.apache.lucene.index.TermPositionVector {

    private String field;
    private String docId;
    private String[] terms;
    private int[] freqVec;
    private int[][] termPositions;
    private TermVectorOffsetInfo[][] termOffsets;

    public TermFreqVector(String indexName, String field, String docId, Cassandra.Iface client) {
        this.field = field;
        this.docId = docId;

        String key = indexName + CassandraUtils.delimeter + docId;

        // Get all terms
        ColumnOrSuperColumn column;
        try {
            column = client.get(CassandraUtils.keySpace, CassandraUtils.hashKey(key), CassandraUtils.metaColumnPath, ConsistencyLevel.ONE);

            List<String> allTermList = (List<String>) CassandraUtils.fromBytes(column.column.value);
            List<String> keys        = new ArrayList<String>();
         
            
            for (String termStr : allTermList) {
                Term t = CassandraUtils.parseTerm(termStr);
             
                // skip the ones not of this field
                if (!t.field().equals(field))
                    continue;
             

                // add to multiget params
                keys.add(CassandraUtils.hashKey(
                        indexName+CassandraUtils.delimeter+termStr
                ));                            
            }

            
            //Fetch all term vectors in this field
            Map<String, ColumnOrSuperColumn> termVec = client.multiget(CassandraUtils.keySpace, keys, new ColumnPath(CassandraUtils.termVecColumnFamily).setColumn(docId.getBytes()), ConsistencyLevel.ONE);
            
            terms    = new String[termVec.size()];
            freqVec  = new int[termVec.size()];
            termPositions = new int[termVec.size()][];
            termOffsets  = new TermVectorOffsetInfo[termVec.size()][];
            
            int i  = 0;
            
            for(Map.Entry<String, ColumnOrSuperColumn> e : termVec.entrySet()){
                String termStr = e.getKey().substring(e.getKey().indexOf(CassandraUtils.delimeter) + CassandraUtils.delimeter.length());
                
                Term t = CassandraUtils.parseTerm(termStr);
            
                terms[i] = t.text();
                  
                byte[] value = e.getValue().column.value; 
                
                termPositions[i] =  value == null ? new int[]{} : CassandraUtils.byteArrayToIntArray(value);
                freqVec[i] = termPositions[i].length;
                termOffsets[i] =  TermVectorOffsetInfo.EMPTY_OFFSET_INFO;/*new TermVectorOffsetInfo[freqVec[i]];
                for(int j=0; j<freqVec[i]; j++)
                    termOffsets[i][j] = new TermVectorOffsetInfo(termPositions[i][j]-1 , termPositions[i][j]+terms[i].length());
                 */
            
                i++;
            }
            
            
        } catch (InvalidRequestException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (NotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (UnavailableException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (TimedOutException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (TException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
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
       
       for(int i=0; i<terms.length; i++){
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
