package lucandra;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermVectorOffsetInfo;
import org.apache.thrift.TException;

public class TermFreqVector implements org.apache.lucene.index.TermFreqVector, org.apache.lucene.index.TermPositionVector {

    private String field;
    private ByteBuffer docId;
    private String[] terms;
    private int[] freqVec;
    private int[][] termPositions;
    private TermVectorOffsetInfo[][] termOffsets;

    public TermFreqVector(byte[] indexName, String field, ByteBuffer docId, Cassandra.Iface client) {
        this.field = field;
        this.docId = docId;


        byte[] docIdBytes = new byte[docId.remaining()];
        System.arraycopy(docId.array(), docId.position()+docId.arrayOffset(), docIdBytes, 0, docIdBytes.length);
        
        
        ByteBuffer key = CassandraUtils.hashKeyBytes(indexName, CassandraUtils.delimeterBytes, docIdBytes);

        // Get all terms
        ColumnOrSuperColumn column;
        try {
            column = client.get( key, CassandraUtils.metaColumnPath, ConsistencyLevel.ONE);

            List<Term> allTermList = (List<Term>) CassandraUtils.fromBytes(column.column.value);
            List<ByteBuffer> keys        = new ArrayList<ByteBuffer>();
         
            
            for (Term t : allTermList) {
             
                // skip the ones not of this field
                if (!t.field().equals(field))
                    continue;
             
                // add to multiget params
                keys.add(CassandraUtils.hashKeyBytes(
                        indexName,CassandraUtils.delimeterBytes,t.field().getBytes("UTF-8"),CassandraUtils.delimeterBytes,t.text().getBytes("UTF-8")
                ));                            
            }

            
            //Fetch all term vectors in this field
            Map<ByteBuffer, List<ColumnOrSuperColumn>> allTermInfo = client.multiget_slice( keys, new ColumnParent(CassandraUtils.termVecColumnFamily).setSuper_column(docId), new SlicePredicate().setSlice_range(new SliceRange(CassandraUtils.emptyByteArray, CassandraUtils.emptyByteArray, false, Integer.MAX_VALUE)), ConsistencyLevel.ONE);
            
            terms         = new String[allTermInfo.size()];
            freqVec       = new int[allTermInfo.size()];
            termPositions = new int[allTermInfo.size()][];
            termOffsets   = new TermVectorOffsetInfo[allTermInfo.size()][];
            
            int i  = 0;
            
            for(Map.Entry<ByteBuffer,List<ColumnOrSuperColumn>> e : allTermInfo.entrySet()){
                
                String ekey = new String(e.getKey().array(), e.getKey().position()+e.getKey().arrayOffset(), e.getKey().remaining(), "UTF-8");
                String termStr = ekey.substring(ekey.indexOf(CassandraUtils.delimeter) + CassandraUtils.delimeter.length());
                
                Term t = CassandraUtils.parseTerm(termStr);
            
                terms[i] = t.text();
            
                //Find the offsets and positions
                Column positionVector = null;
                Column offsetVector   = null;
                
                //List<Column> columns  = e.getValue().get(0).getSuper_column().getColumns();
                for(ColumnOrSuperColumn c : e.getValue()){
                    
                    if(Arrays.equals(c.column.getName(), CassandraUtils.positionVectorKey.getBytes()))
                        positionVector = c.column;
                    
                    if(Arrays.equals(c.column.getName(), CassandraUtils.offsetVectorKey.getBytes()))
                        offsetVector   = c.column;
                    
                }
            
                
                termPositions[i] = positionVector == null ? new int[]{} : CassandraUtils.byteArrayToIntArray(positionVector.value);
                freqVec[i]       = termPositions[i].length;
                
                if(offsetVector == null){
                    termOffsets[i] = TermVectorOffsetInfo.EMPTY_OFFSET_INFO;
                }else{
                    
                    int[] offsets  = CassandraUtils.byteArrayToIntArray(offsetVector.value);
                    
                    termOffsets[i] = new TermVectorOffsetInfo[freqVec[i]];
                    for(int j=0,k=0; j<offsets.length; j+=2,k++){
                        termOffsets[i][k] = new TermVectorOffsetInfo(offsets[j] , offsets[j+1]);
                    }
                }
        
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
