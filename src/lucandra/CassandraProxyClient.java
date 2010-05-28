/**
 * Copyright 2010 T Jake Luciani
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
package lucandra;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Random;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

/**
 * This wraps the underlying Cassandra thrift client and attempts to handle disconnect errors.
 * 
 * On disconnect, if it cannot reconnect to the same host then it will use a different host from the ring,
 * which it periodically checks for updates to.
 * 
 * This incorporates the CircuitBreaker pattern so not to overwhelm the network with reconnect attempts.
 * 
 * @author jake
 *
 */
public class CassandraProxyClient implements java.lang.reflect.InvocationHandler {

    private static final Logger logger = Logger.getLogger(CassandraProxyClient.class);
    private String host;
    private int port;
    private boolean framed;
    private Cassandra.Client client;
    private long lastPoolCheck;
    private List<TokenRange> ring;
    private CircuitBreaker circuitBreaker;
    
    public static Cassandra.Iface newInstance(String host, int port, boolean framed) {
                    
        return (Cassandra.Iface) java.lang.reflect.Proxy.newProxyInstance(Cassandra.Client.class.getClassLoader(), Cassandra.Client.class.getInterfaces(), new CassandraProxyClient(host,port,framed));
    }

    private CassandraProxyClient(String host, int port, boolean framed) {
        
        this.host = host;
        this.port = port;
        this.framed = framed;
        lastPoolCheck  = 0;
        circuitBreaker = new CircuitBreaker(1, 1);
        
        try {
            this.client    = CassandraUtils.createConnection(host, port, framed);
        } catch (TTransportException e) {
             circuitBreaker.failure();
        }
        
        checkRing();
    }

    private synchronized void checkRing() {
        
        if(client == null){
            circuitBreaker.failure();
            return;
        }
        
        long now = System.currentTimeMillis();
        
        if( (now - lastPoolCheck) > 60*1000){
            try {
                if(circuitBreaker.allow()){
                    ring = client.describe_ring(CassandraUtils.keySpace);
                    lastPoolCheck = now;
                    circuitBreaker.success();
                }
            } catch (TException e) {
                circuitBreaker.failure();
                attemptReconnect();
            }
        }
        
    }
    
    private void attemptReconnect(){
        
        //first try to connect to the same host as before
        try {
            client = CassandraUtils.createConnection(host, port, framed);
            circuitBreaker.success();
            logger.info("Reconnected to cassandra at "+host+":"+port);
            return;
        } catch (TTransportException e) {            
            logger.warn("Connection failed to Cassandra node: "+host+":"+port);
        }
        
        //this is bad
        if(ring == null || ring.size() == 0){ 
            logger.warn("No cassandra ring information found, no other nodes to connect to");
            return;
        }
        
        //pick a different node from the ring
        Random r = new Random();
        
        List<String> endpoints = ring.get(r.nextInt(ring.size())).endpoints;
        String endpoint = endpoints.get(r.nextInt(endpoints.size()));

        //only one node (myself)
        if(endpoint.equals(host) && ring.size() == 1){
            logger.warn("No other cassandra nodes in this ring to connect to");
            return;
        }
        
        //make sure this is a node other than ourselves
        while(endpoint.equals(host)){
            endpoint = endpoints.get(r.nextInt(endpoints.size()));       
        }
        
        try {
            client = CassandraUtils.createConnection(endpoint, port, framed);
            host = endpoint;
            circuitBreaker.success();
            return;
        } catch (TTransportException e) {            
            logger.warn("Failed connecting to a different cassandra node in this ring: "+endpoint+":"+port);
        }
         
        logger.info("Reconnected to cassandra at "+host+":"+port);       
    }
    
    public Object invoke(Object proxy, Method m, Object[] args) throws Throwable {
        Object result = null;
        
        checkRing();  
        
        int tries    = 0;
        int maxTries = 4;    
        
        while(result == null && ++tries < maxTries){
            
            //don't even try if client isn't connected
            if(client == null) {
                circuitBreaker.failure();
            }
            
            try {
                
                if(circuitBreaker.allow()){                
                    result = m.invoke(client, args);
                    circuitBreaker.success();
                }else{
                    while(!circuitBreaker.allow()){
                        Thread.sleep(1050); //sleep and try again
                    }
                    
                    attemptReconnect();
                }
            } catch (InvocationTargetException e) {
            
                if(e.getTargetException() instanceof UnavailableException){
                
                    circuitBreaker.failure();                   
                    attemptReconnect();                   
                
                }
                
                if(e.getTargetException() instanceof TTransportException){
                    circuitBreaker.failure();
                    attemptReconnect();
                }
            
                throw e.getTargetException();
        
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("unexpected invocation exception: " + e.getMessage(),e);
            } finally {
                //System.out.println("after method " + m.getName());
            }
        }
        
        
        if(tries > maxTries){
            throw new UnavailableException();
        }
        
        //incase this is the first time
        if(ring == null)
            checkRing();
        
        return result;
    }
    
    
}
