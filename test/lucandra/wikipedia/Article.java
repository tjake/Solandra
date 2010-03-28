package lucandra.wikipedia;

public class Article {
    public String  url;
    public String  title;
    public byte[]  text;
    
    public int getSize(){
        return url.length() + title.length() + (text == null ? 0 : text.length);
    }
}
