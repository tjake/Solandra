package solandra;

import java.util.List;
import java.util.Set;

import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.FieldSelectorResult;

public class SolandraFieldSelector implements FieldSelector {

    List<Integer> otherDocsToCache;
    Set<String> fieldNames;
    
    public SolandraFieldSelector(List<Integer> otherDocsToCache, Set<String> fieldNames){
        this.otherDocsToCache = otherDocsToCache;
        this.fieldNames = fieldNames;
    }
    
    public List<Integer> getOtherDocsToCache(){
        return otherDocsToCache;
    }
    
    public Set<String> getFieldNames(){
        return fieldNames;
    }
    
    public FieldSelectorResult accept(String fieldName) {
        // TODO Auto-generated method stub
        return null;
    }

}
