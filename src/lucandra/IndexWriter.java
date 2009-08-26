package lucandra;

import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.service.BatchMutationSuper;
import org.apache.cassandra.service.Cassandra;
import org.apache.cassandra.service.Column;
import org.apache.cassandra.service.ColumnParent;
import org.apache.cassandra.service.ConsistencyLevel;
import org.apache.cassandra.service.InvalidRequestException;
import org.apache.cassandra.service.SuperColumn;
import org.apache.cassandra.service.UnavailableException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Similarity;
import org.apache.thrift.TException;

import com.sun.xml.internal.fastinfoset.algorithm.UUIDEncodingAlgorithm;

public class IndexWriter {

    private final String keySpace = "Lucandra";
    private final String indexName;
    private final Analyzer analyzer;
    private final Cassandra.Client client;
    private final Similarity similarity;

    public IndexWriter(String name, Analyzer a, Cassandra.Client client)  {

        this.indexName = name;
        this.analyzer = a;
        this.similarity = Similarity.getDefault();
        this.client = client;

    }

    public void addDocument(Document doc) throws CorruptIndexException, IOException {

        Token token = new Token();
       
        //Build wacky batch struct
        BatchMutationSuper inserts = new BatchMutationSuper();
        inserts.setKey(indexName);
        
        Map<String, List<SuperColumn>> cfMap = new HashMap<String, List<SuperColumn>>();
        inserts.setCfmap(cfMap);
        
        List<SuperColumn> terms = new ArrayList<SuperColumn>();
        cfMap.put("Terms", terms);
        
        List<SuperColumn> docs = new ArrayList<SuperColumn>();
        cfMap.put("Documents", docs);
              
        byte[] docId = CassandraUtils.randomUUID();
        
        SuperColumn docColumn = new SuperColumn();
        docColumn.setName(docId);
        docColumn.setColumns(new ArrayList<Column>());
        docs.add(docColumn);
        
        for (Field field : (List<Field>) doc.getFields()) {

            // Indexed field
            if (field.isIndexed() && field.isTokenized()) {

                TokenStream tokens = field.tokenStreamValue();

                if (tokens == null) {
                    tokens = analyzer.tokenStream(field.name(), new StringReader(field.stringValue()));
                }

                //collect term frequencies per doc
                Map<String,Integer> termDocFreq = new HashMap<String,Integer>();
                
                while (tokens.next(token) != null) {
                    //may need to l/r pad this
                    String term = CassandraUtils.createColumnName(field.name(), token.term());
                    
                    Integer freq = termDocFreq.get(term);
                    
                    if(freq == null){
                        freq = new Integer(0);                      
                    }
                    
                    termDocFreq.put(term, ++freq);
                }
            
                for(Map.Entry<String,Integer> term : termDocFreq.entrySet()){
                    SuperColumn termColumn = new SuperColumn();
                    termColumn.setName(term.getKey().getBytes());
                    termColumn.setColumns(new ArrayList<Column>());
                   
                    //Add to terms table
                    terms.add(termColumn);                  
                                     
                    Column idColumn = new Column(docId,CassandraUtils.intToByteArray(term.getValue()),System.currentTimeMillis());
                    
                    //add to termColumn
                    termColumn.getColumns().add(idColumn);                   
                }
            }
            
            if( field.isStored() ) {
              
                byte[] value = field.isBinary() ? field.getBinaryValue() : field.stringValue().getBytes();
                
                Column fieldColumn = new Column(field.name().getBytes(), value, System.currentTimeMillis());
                
                docColumn.getColumns().add(fieldColumn);
            }
        }
        
        //send document
        try {
            client.batch_insert_super_column(keySpace, inserts, ConsistencyLevel.ONE);
        } catch (TException e) {
            throw new RuntimeException(e);
        } catch (InvalidRequestException e) {
            throw new RuntimeException(e);
        } catch (UnavailableException e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteDocuments(Query query) throws CorruptIndexException, IOException {
        
    }

    public void deleteDocuments(Term arg0) throws CorruptIndexException, IOException {

    }

    public int docCount() {
        ColumnParent columnParent = new ColumnParent();
        columnParent.setSuper_column("Documents".getBytes());

        try {
            return client.get_count(keySpace, indexName, columnParent, ConsistencyLevel.ONE);
        } catch (TException e) {
            throw new RuntimeException(e);
        } catch (InvalidRequestException e) {
            throw new RuntimeException(e);
        }

    }

}
