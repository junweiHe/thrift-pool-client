package com.thrift.client.config;

import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;

public class KeyPoolConfig extends GenericKeyedObjectPoolConfig{
	private int timeout = 0;

    private boolean failover = false;

    /**
     * get default connection socket timeout (default 0, means not timeout)
     * 
     * @return
     */
    public int getTimeout() {
        return timeout;
    }

    /**
     * set default connection socket timeout
     * 
     * @param timeout timeout millis
     */
    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    /**
     * get connect to next service if one service fail(default false)
     * 
     * @return
     */
    public boolean isFailover() {
        return failover;
    }

    /**
     * set connect to next service if one service fail
     * 
     * @param failover
     */
    public void setFailover(boolean failover) {
        this.failover = failover;
    }
}	
