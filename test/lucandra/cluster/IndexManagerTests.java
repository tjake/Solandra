/**
 * Copyright T Jake Luciani
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package lucandra.cluster;

import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.*;

import lucandra.CassandraUtils;

import org.junit.BeforeClass;
import org.junit.Test;


public class IndexManagerTests
{
    static String indexName = String.valueOf(System.nanoTime());
    
    private class TestCassandraIndexManager extends CassandraIndexManager
    {

        
        
        public TestCassandraIndexManager(int shardsAtOnce)
        {
            super(shardsAtOnce);
            // TODO Auto-generated constructor stub
        }
        
        public String getToken()
        {
            return Thread.currentThread().getName();
        }
        
    }
    
    @BeforeClass
    public static void setUpBeforeClass() {       
            // start cassandra
            CassandraUtils.startup();
    }
    
    @Test
    public void testCassandraIncrement()
    {
        
        CassandraIndexManager idx = new CassandraIndexManager(1);
        
        Set<Long> all = new HashSet<Long>(CassandraUtils.maxDocsPerShard);
        
        long startTime = System.currentTimeMillis();
        
        //Add
        for(int i=0; i<CassandraUtils.maxDocsPerShard; i++)
        {
            long id = idx.getNextId(indexName, "i"+i);
              
            assertTrue(id+" already exists "+all.size(),all.add(id));
            
            if(i % 10000 == 0){
                long endTime = System.currentTimeMillis();
                System.err.println("added:"+id+", 10k iterations in "+(endTime - startTime)/1000+" sec");
                startTime = endTime;         
            }
        }
        
        
        assertEquals(0, CassandraIndexManager.getShardFromDocId(idx.getMaxId(indexName)));
        
        //Update
        for(int i=0; i<CassandraUtils.maxDocsPerShard; i++)
        {
            Long id = idx.getId(indexName, "i"+i);
            
            
            assertNotNull(id);
            
            if(i % 10000 == 0){
                long endTime = System.currentTimeMillis();
                System.err.println("updated:"+id+", 10k iterations in "+(endTime - startTime)/1000+" sec");
                startTime = endTime;      
            }

        }
    }
    
    
    @Test
    public void testCassandraIncrement2()
    {
        
        indexName = String.valueOf(System.nanoTime());

        ExecutorService svc = Executors.newFixedThreadPool(16);
        
        final TestCassandraIndexManager idx = new TestCassandraIndexManager(1);

        
        List<Callable<Set<Long>>> callables = new ArrayList<Callable<Set<Long>>>();
        for(int i=0; i<16; i++){
            Callable<Set<Long>> r = new Callable<Set<Long>>() {
                
                public Set<Long> call()
                {

                   long startTime = System.currentTimeMillis();
                    
                    Set<Long> all = new HashSet<Long>(CassandraUtils.maxDocsPerShard);

                    for(int i=0; i<CassandraUtils.maxDocsPerShard/10; i++)
                    {
                        Long id = idx.getNextId(indexName, "i"+i);
                        assertTrue(id+" already exists "+all.size(),all.add(id));
                        
                        if(i % 10000 == 0){
                            long endTime = System.currentTimeMillis();
                            System.err.println(Thread.currentThread().getName()+" id:"+id+", 10k iterations in "+(endTime - startTime)/1000+" sec");
                            startTime = endTime;      
                        }
                    }
                    
                    return all;
                }

             
            };
            
            callables.add(r);
        }
        
        try
        {
            List<Future<Set<Long>>> results = svc.invokeAll(callables);
            
            Set<Long> all = new HashSet<Long>(CassandraUtils.maxDocsPerShard);
            
            for(Future<Set<Long>> result : results)
            {
                Set<Long> thread = result.get();
                
                for(Long id : thread) {
                    if(!all.add(id)){
                        System.err.println(id+" already exists "+all.size());
                    }
                }
            }            
        }
        catch (InterruptedException e1)
        {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        catch (ExecutionException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        svc.shutdown();
        
        try
        {
            svc.awaitTermination(10, TimeUnit.MINUTES);
        }
        catch (InterruptedException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        
    }
    
 
}
