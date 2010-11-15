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

import org.apache.log4j.Logger;

/**
 * Simple state machine which acts like household circuit breaker.
 * 
 * Starts CLOSED. 
 * If a "surge" or error threshold is reached then it will OPEN.
 * While OPEN allow() will fail till timeout has been reached.
 * Once reached it will go HALF-OPEN and allow one try 
 * If successful() will go fully CLOSED else back to OPEN.
 * 
 * @author jake //from thrudb
 *
 */
public class CircuitBreaker {
    private enum State {
        CLOSED,
        HALF_OPEN,
        OPEN
    }

    private static final Logger logger = Logger.getLogger(CircuitBreaker.class);
    private State state     = State.CLOSED;
    private long  nextCheck = 0;

    private int   failureRate   = 0;
    private int   threshold     = 1;
    private int   timeoutInSecs = 1;


    public CircuitBreaker(int threshold, int timeoutInSecs){
        if(threshold > 0)
            this.threshold = threshold;

        if(timeoutInSecs > 0)
            this.timeoutInSecs = timeoutInSecs;

    }

    public boolean allow() {

        if(state == State.OPEN && nextCheck < System.currentTimeMillis()/1000){
            if(logger.isDebugEnabled())
                logger.debug("allow:  going half-open");
            
            state = State.HALF_OPEN;
        }

        return state == State.CLOSED || state == State.HALF_OPEN;               
    }

    public void success(){
        if(state == State.HALF_OPEN){
            reset();
        }
    }

    public void failure(){
        if(state == State.HALF_OPEN){
            if(logger.isDebugEnabled())
                logger.debug("failure: in half-open, trip");
            
            trip();
        } else {
            ++failureRate;

            if(failureRate >= threshold){
                if(logger.isDebugEnabled())
                    logger.debug("failure: reached threash, tripped");
                
                trip(); 
            }
        }
    }

    private void reset(){
        state       = State.CLOSED;
        failureRate = 0;        
    }

    private void trip(){

        if(state != State.OPEN){
            if(logger.isDebugEnabled())
                logger.debug("trip: tripped");

            state = State.OPEN;
            nextCheck = System.currentTimeMillis()/1000 + timeoutInSecs;
        }

    }
}


