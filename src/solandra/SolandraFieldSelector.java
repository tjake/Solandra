package solandra;

import java.util.List;

import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.FieldSelectorResult;

public class SolandraFieldSelector implements FieldSelector {

    List<Integer> otherDocsToCache;
    List<byte[]> fieldNames;
    
    public SolandraFieldSelector(List<Integer> otherDocsToCache, List<byte[]> fieldNames){
        this.otherDocsToCache = otherDocsToCache;
        this.fieldNames = fieldNames;
    }
    
    public List<Integer> getOtherDocsToCache(){
        return otherDocsToCache;
    }
    
    public List<byte[]> getFieldNames(){
        return fieldNames;
    }
    
    public FieldSelectorResult accept(String fieldName) {
        // TODO Auto-generated method stub
        return null;
    }

}
