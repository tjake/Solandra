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
import org.apache.cassandra.service.ColumnParent;
import org.apache.cassandra.service.ConsistencyLevel;
import org.apache.cassandra.service.InvalidRequestException;
import org.apache.cassandra.service.SuperColumn;
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

    public IndexWriter(String indexName, Cassandra.Client client)  {

        this.indexName = indexName;
        this.client    = client;

    }

    public void addDocument(Document doc, Analyzer analyzer) throws CorruptIndexException, IOException {

        Token token = new Token();
       
        //Build wacky batch struct
        //BatchMutation inserts = new BatchMutation();
        //inserts.setKey(indexName);
        
        Map<String, List<ColumnOrSuperColumn>> cfMap = new HashMap<String, List<ColumnOrSuperColumn>>();
        //inserts.setCfmap(cfMap);      
        
        List<ColumnOrSuperColumn> termVec = new ArrayList<ColumnOrSuperColumn>();
        cfMap.put(CassandraUtils.termVecColumn, termVec);
        
        List<ColumnOrSuperColumn> docs = new ArrayList<ColumnOrSuperColumn>();
        cfMap.put(CassandraUtils.docColumn, docs);
        
        
        byte[] docId = CassandraUtils.encodeLong(System.nanoTime());
        
        SuperColumn docColumn = new SuperColumn();
        docColumn.setName(docId);
        docColumn.setColumns(new ArrayList<Column>());
        docs.add(new ColumnOrSuperColumn(null,docColumn));
        
        int position = 0;
        
        for (Field field : (List<Field>) doc.getFields()) {

            // Indexed field
            if (field.isIndexed() && field.isTokenized()) {

                TokenStream tokens = field.tokenStreamValue();

                if (tokens == null) {
                    tokens = analyzer.tokenStream(field.name(), new StringReader(field.stringValue()));
                }

                //collect term frequencies per doc
                Map<String,List<Integer>> termPositions = new HashMap<String,List<Integer>>();
                int lastOffset = 0;
                if(position > 0){
                    position += analyzer.getPositionIncrementGap(field.name());
                }
                
                while (tokens.next(token) != null) {                   
                    String term = CassandraUtils.createColumnName(field.name(), token.term());
                    
                    List<Integer> pvec = termPositions.get(term);
                    
                    if(pvec == null){
                        pvec = new ArrayList<Integer>();
                        termPositions.put(term, pvec);
                    }
                    
                    position += (token.getPositionIncrement() - 1);
                    pvec.add(++position);
                    
                }
            
                for(Map.Entry<String,List<Integer>> term : termPositions.entrySet()){

                    SuperColumn termVecColumn = new SuperColumn(term.getKey().getBytes(), new ArrayList<Column>());
                    
                    //Add to terms table
                    termVec.add(new ColumnOrSuperColumn(null,termVecColumn));
                                     
                    //stores the freq count
                    Column vecColumn  = new Column(docId,CassandraUtils.intVectorToByteArray(term.getValue()),System.currentTimeMillis());
                    
                    //add to termColumn
                    termVecColumn.getColumns().add(vecColumn);
                }
            }
            
            if( field.isIndexed() && !field.isTokenized()) {
                String term = CassandraUtils.createColumnName(field.name(), field.stringValue());
                
                SuperColumn termVecColumn = new SuperColumn(term.getBytes(), new ArrayList<Column>());
                
                //Add to terms table
                termVec.add(new ColumnOrSuperColumn(null,termVecColumn));
                                 
                //stores the freq count
                Column vecColumn  = new Column(docId,CassandraUtils.intVectorToByteArray(Arrays.asList(new Integer[]{0})),System.currentTimeMillis());
                
                //add to termColumn
                termVecColumn.getColumns().add(vecColumn);
            }
            
            if( field.isStored() ) {
              
                byte[] value = field.isBinary() ? field.getBinaryValue() : field.stringValue().getBytes();
                
                Column fieldColumn = new Column(field.name().getBytes(), value, System.currentTimeMillis());
                
                docColumn.getColumns().add(fieldColumn);
            }
        }
        
        //send document
        try {
            long startTime = System.currentTimeMillis();
            client.batch_insert(CassandraUtils.keySpace, indexName,cfMap, ConsistencyLevel.ZERO);
            logger.info("Inserted in "+(startTime - System.currentTimeMillis())/1000+"ms" );
        } catch (TException e) {
            throw new RuntimeException(e);
        } catch (InvalidRequestException e) {
            throw new RuntimeException(e);
        } catch (UnavailableException e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteDocuments(Query query) throws CorruptIndexException, IOException {
        throw new UnsupportedOperationException();
    }

    public void deleteDocuments(Term arg0) throws CorruptIndexException, IOException {
        throw new UnsupportedOperationException();
    }

    public int docCount() {
        ColumnParent columnParent = new ColumnParent();
        columnParent.setColumn_family(CassandraUtils.docColumn);

        try {
            return client.get_count(CassandraUtils.keySpace, indexName, columnParent, ConsistencyLevel.ONE);    
        } catch (TException e) {
            throw new RuntimeException(e);
        } catch (InvalidRequestException e) {
            throw new RuntimeException(e);
        } catch (UnavailableException e) {
            throw new RuntimeException(e);
        }

    }

}
