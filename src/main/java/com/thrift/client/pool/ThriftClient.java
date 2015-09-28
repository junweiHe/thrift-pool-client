package com.thrift.client.pool;

import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.commons.pool2.ObjectPool;
import org.apache.thrift.TServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

/**
 * The thrift client which hold the connection to backend server.<br/>
 * 
 * ThriftClient is not thread-safe, you must obtain separately from
 * {@link ThriftClientPool} for each thread.
 * 
 * @author javamonk
 * @createTime 2014年7月4日 下午3:50:51
 */
public class ThriftClient<T extends TServiceClient> implements Closeable {
	private static Logger log = LoggerFactory.getLogger(ThriftClient.class.getName());
    private final T client;

    private final ObjectPool<ThriftClient<T>> pool;
    private final KeyedObjectPool<ServiceInfo,ThriftClient<T>> keyPool;
    private final ServiceInfo serviceInfo;

    private boolean finish;

    public ThriftClient(T client, ObjectPool<ThriftClient<T>> pool,
            ServiceInfo serviceInfo) {
        super();
        this.client = client;
        this.pool = pool;
        this.keyPool = null;
        this.serviceInfo = serviceInfo;
    }
    public ThriftClient(T client,KeyedObjectPool<ServiceInfo,ThriftClient<T>> pool,ServiceInfo serviceInfo){
    	super();
    	this.client = client;
    	this.keyPool = pool;
    	this.pool = null;
    	this.serviceInfo = serviceInfo;
    }
    /**
     * get backend service which this client current connect to
     * 
     * @return
     */
    public ServiceInfo getServiceInfo() {
        return serviceInfo;
    }

    /**
     * Retrieve the IFace
     * 
     * @return
     */
    public T iFace() {
        return client;
    }

    @Override
    public void close() {
        try {
            if (finish) {
                log.info("return object to pool: " + this);
                finish = false;
                if(pool!=null)
                	pool.returnObject(this);
                else{
                	keyPool.returnObject(serviceInfo, this);
                }
            } else {
                log.warn("not return object cause not finish {}", client);
                closeClient();
                if(pool!=null)
                	pool.invalidateObject(this);
                else{
                	keyPool.invalidateObject(serviceInfo, this);
                }
            }
        } catch (Exception e) {
            log.warn("return object fail, close", e);
            closeClient();
        }
    }

    void closeClient() {
        log.debug("close client {}", this);
        ThriftUtil.closeClient(this.client);
    }

    /**
     * client should return to pool
     * 
     */
    public void finish() {
        this.finish = true;
    }

    void setFinish(boolean finish) {
        this.finish = finish;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        closeClient();
    }
}
