package com.thrift.client.pool;


/**
 * @author javamonk
 * @createTime 2014年11月22日 下午2:30:39
 */
public class ServiceInfo {

	private String host;

    private int port;
    
    public ServiceInfo(String host,int port){
    	this.host = host;
    	this.port = port;
    }

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	@Override
	public int hashCode() {
		return host.hashCode()+port;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj != null && obj instanceof ServiceInfo){
			ServiceInfo other = (ServiceInfo) obj;
			return other.getHost().equals(this.host) && other.getPort() == this.port;
		}
		return false;
	}
    
}
