package lucandra.wikipedia;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

public class WikipediaImporter {
    
    private ExecutorService threadPool;
    private Queue<Future<Boolean>> resultSet;
    private int pageCount;
    
    
    public WikipediaImporter() {
        threadPool = Executors.newFixedThreadPool(16);
        resultSet  = new LinkedBlockingQueue<Future<Boolean>>();
        pageCount = 0;
    }
    
    private static void usage(){
        System.err.println("WikipediaImporter file.xml.bz2");
        System.exit(0);
    }
    

    private void readFile(String fileName) throws IOException{
        
        
        InputStream    inputFile  = new FileInputStream(fileName);   
        BufferedReader fileStream = new BufferedReader(new InputStreamReader(inputFile));
        
        //rather than xml parse, just do something fast & simple.
        String line;
        Article page = new Article();
        boolean inText = false;

        while( (line = fileStream.readLine()) != null){
            
            //Page
            if(line.contains("<doc>")){
                page = new Article();           
                continue;
            }
            
            if(line.contains("</doc>")){
               
               if(pageCount++ % 1000 == 0){
                    System.err.println("Loaded "+pageCount);
                    
                    Future<Boolean> result;
                    while( (result = resultSet.poll()) != null){
                        try {
                            result.get();
                        } catch (InterruptedException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        } catch (ExecutionException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }
                    
               }
               
               indexPage(page);  //index each page           
            }
            
            //title
            if(line.contains("<title>")){
                page.title = line.substring(line.indexOf("<title>")+7, line.indexOf("</title>"));
               
                continue;
            }
            
            //url
            if(line.contains("<url>")){
                if(page.url == null)
                    page.url     = line.substring(line.indexOf("<url>")+5,line.indexOf("</url>"));
                
                continue;
            }
            
            
            //article text
            if(line.contains("<abstract>")){
                
                if(line.contains("</abstract>")){
                    page.text = line.substring(line.indexOf("<abstract>")+10, line.indexOf("</abstract>")).getBytes();
                }else{              
                    page.text = line.substring(line.indexOf("<abstract>"+10)).getBytes();
                    inText = true;
                    continue;
                }
            }
            
                      
            if(inText){
                
                String text = line;
                if(line.contains("</abstract>"))
                    text = line.substring(0,line.indexOf("</abstract>"));
                
                
                byte[] newText = new byte[page.text.length+text.getBytes().length];
                    
                System.arraycopy(page.text, 0, newText, 0, page.text.length);
                System.arraycopy(text.getBytes(), 0, newText, page.text.length, text.getBytes().length);
                    
                page.text = newText;
            }       
            
            
            if(line.contains("</abstract>")){
                inText = false;
                continue;
            }
        }
    }
    
    public void indexPage(Article page){
        
       Future<Boolean> result = threadPool.submit(new WikipediaIndexWorker(page));     
       resultSet.add(result); 
       
    }
    
    public static void main(String[] args){
        
        
        try {
            
        
            new WikipediaImporter().readFile(args[0]);
           
           
            
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e){
            e.printStackTrace();
        }
        
        
    }
    
}
