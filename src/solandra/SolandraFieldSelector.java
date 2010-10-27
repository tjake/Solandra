package solandra;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.FieldSelectorResult;

public class SolandraFieldSelector implements FieldSelector {

    List<Integer> otherDocsToCache;
    List<ByteBuffer> fieldNames;
    
    public SolandraFieldSelector(List<Integer> otherDocsToCache, List<ByteBuffer> fieldNames){
        this.otherDocsToCache = otherDocsToCache;
        this.fieldNames = fieldNames;
    }
    
    public List<Integer> getOtherDocsToCache(){
        return otherDocsToCache;
    }
    
    public List<ByteBuffer> getFieldNames(){
        return fieldNames;
    }
    
    public FieldSelectorResult accept(String fieldName) {
        // TODO Auto-generated method stub
        return null;
    }

}
