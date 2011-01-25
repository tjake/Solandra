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
import org.apache.cassandra.thrift.InvalidRequestException;
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
 * This proxy also manages a client connection per thread.
 * 
 * @author jake
 *
 */
public class CassandraProxyClient implements java.lang.reflect.InvocationHandler {

    private static final Logger logger = Logger.getLogger(CassandraProxyClient.class);
    private String host;
    private int port;
    private final boolean framed;
    private final boolean randomizeConnections;
    private static ThreadLocal<Cassandra.Iface> clientPool = new ThreadLocal<Cassandra.Iface>();
    private long lastPoolCheck;
    private List<TokenRange> ring;
    private static ThreadLocal<CircuitBreaker> circuitBreakerPool = new ThreadLocal<CircuitBreaker>();
    
    public static Cassandra.Iface newInstance(String host, int port, boolean framed, boolean randomizeConnections) {
                    
        return (Cassandra.Iface) java.lang.reflect.Proxy.newProxyInstance(Cassandra.Client.class.getClassLoader(), Cassandra.Client.class.getInterfaces(), new CassandraProxyClient(host,port,framed,randomizeConnections));
    }

    private CassandraProxyClient(String host, int port, boolean framed, boolean randomizeConnections) {
        
        this.host = host;
        this.port = port;
        this.framed = framed;
        this.randomizeConnections = randomizeConnections;
        lastPoolCheck  = 0;              

        checkRing();
    }

    private CircuitBreaker getCircuitBreaker(){
        CircuitBreaker breaker = circuitBreakerPool.get();
        
        if(breaker == null){
            breaker = new CircuitBreaker(1, 1);
    
            circuitBreakerPool.set(breaker);
        }
    
        return breaker;
    }
    
    private Cassandra.Iface getClient(){
        Cassandra.Iface client = clientPool.get();       
        
        if(client == null){
            CircuitBreaker breaker = getCircuitBreaker();
            
            if(breaker.allow())
                client = attemptReconnect();          
            
            if(client == null)
                return null;
            
            clientPool.set(client);
        }
                
        return client;
    }
    
    private void setClient(Cassandra.Iface client)
    {
        clientPool.set(client);
    }
    
    private synchronized void checkRing() {
        
        Cassandra.Iface client  = getClient();
        CircuitBreaker  breaker = getCircuitBreaker();
        
        if(client == null){
            breaker.failure();
            return;
        }
        
        long now = System.currentTimeMillis();
        
        if( (now - lastPoolCheck) > 60*1000){
            try {
                if(breaker.allow()){
                    ring = client.describe_ring(CassandraUtils.keySpace);
                    lastPoolCheck = now;
	       
                    breaker.success();
                }
            } catch (TException e) {
                breaker.failure();
                attemptReconnect();
            } catch (InvalidRequestException e) {
               throw new RuntimeException(e);
            }
        }
        
    }
    
    private Cassandra.Iface attemptReconnect(){

            
        Cassandra.Client client;
        CircuitBreaker   breaker = getCircuitBreaker();
        
        //first try to connect to the same host as before
        if(!randomizeConnections || ring == null || ring.size() == 0) {
            
            try {
                client = CassandraUtils.createConnection(host, port, framed);
                breaker.success();
                logger.info("Connected to cassandra at "+host+":"+port);
                setClient(client);
                return client;
            } catch (TTransportException e) {            
                logger.warn("Connection failed to Cassandra node: "+host+":"+port+" "+e.getMessage());
            }
        }
        
        //this is bad
        if(ring == null || ring.size() == 0){ 
            logger.warn("No cassandra ring information found, no other nodes to connect to");
            return null;
        }
        
        //pick a different node from the ring
        Random r = new Random();
        
        List<String> endpoints = ring.get(r.nextInt(ring.size())).endpoints;
        String endpoint = endpoints.get(r.nextInt(endpoints.size()));

        if(!randomizeConnections){
            //only one node (myself)
            if(endpoint.equals(host) && ring.size() == 1){
                logger.warn("No other cassandra nodes in this ring to connect to");
                return null;
            }
        
            //make sure this is a node other than ourselves
            while(endpoint.equals(host)){
                endpoint = endpoints.get(r.nextInt(endpoints.size()));       
            }
        }
        
        try {
            client = CassandraUtils.createConnection(endpoint, port, framed);
            //host = endpoint;
            breaker.success();
            logger.info("Connected to cassandra at "+endpoint+":"+port);               
        } catch (TTransportException e) {            
            logger.warn("Failed connecting to a different cassandra node in this ring: "+endpoint+":"+port);
            
            try {
                client = CassandraUtils.createConnection(host, port, framed);
                breaker.success();
                logger.info("Connected to cassandra at "+host+":"+port);
            } catch (TTransportException e2) {            
                logger.warn("Connection failed to Cassandra node: "+host+":"+port);
            }
            
            return null;
        }
         
        
        setClient(client);
        return client;
    }
    
    public Object invoke(Object proxy, Method m, Object[] args) throws Throwable {
        Object result = null;
        
        Cassandra.Iface client = getClient();
        CircuitBreaker breaker = getCircuitBreaker();
        
        int tries    = 0;
        int maxTries = 4;    
        
        while(result == null && ++tries < maxTries){
            
            //don't even try if client isn't connected
            if(client == null) {
                breaker.failure();
            }
            
            try {
                
                if(breaker.allow()){                
                    result = m.invoke(client, args);
                    breaker.success();
                    return result;
                }else{
                    
                    while(!breaker.allow()){
                        Thread.sleep(1050); //sleep and try again
                    }
                    
                    attemptReconnect();
                }
            } catch (InvocationTargetException e) {
            
                if(e.getTargetException() instanceof UnavailableException){
                
                    breaker.failure();                   
                    attemptReconnect();                   
                
                }
                
                if(e.getTargetException() instanceof TTransportException){
                    breaker.failure();
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
