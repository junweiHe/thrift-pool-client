package com.thrift.client.pool;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thrift.client.config.KeyPoolConfig;
import com.thrift.client.exception.ConnectionFailException;
import com.thrift.client.exception.NoBackendServiceException;
import com.thrift.client.exception.ThriftException;

public class ThriftClientKeyPool<T extends TServiceClient> {

	private static Logger log = LoggerFactory
			.getLogger(ThriftClientKeyPool.class.getName());

	private final ThriftClientFactory<T> clientFactory;

	private final ThriftProtocolFactory protocolFactory;

	private final GenericKeyedObjectPool<ServiceInfo, ThriftClient<T>> pool;

	private List<ServiceInfo> services;

//	private boolean serviceReset = false;

	private final KeyPoolConfig poolConfig;
	private final static int MAXFFILOVERNUM = 3;

	public ThriftClientKeyPool(List<ServiceInfo> services,
			ThriftClientFactory<T> factory) {
		this(services, factory, new KeyPoolConfig(), null);
	}

	public ThriftClientKeyPool(List<ServiceInfo> services,
			ThriftClientFactory<T> factory, KeyPoolConfig config,
			ThriftProtocolFactory pFactory) {
		if (services == null || services.size() == 0) {
			throw new IllegalArgumentException("services is empty!");
		}
		if (factory == null) {
			throw new IllegalArgumentException("factory is empty!");
		}
		if (config == null) {
			throw new IllegalArgumentException("config is empty!");
		}

		this.services = services;
		this.clientFactory = factory;
		this.poolConfig = config;
		this.protocolFactory = pFactory == null ? new ThriftBinaryProtocolFactory()
				: pFactory;
		// test if config change
		this.poolConfig.setTestOnReturn(true);
		this.poolConfig.setTestOnBorrow(true);

		this.pool = new GenericKeyedObjectPool<>(
				new BaseKeyedPooledObjectFactory<ServiceInfo, ThriftClient<T>>() {

					@Override
					public ThriftClient<T> create(ServiceInfo key)
							throws Exception {
						TTransport transport = getTransport(key);
						try {
							transport.open();
						} catch (TTransportException e) {
							log.info(
									"transport open fail service: host={}, port={}",
									key.getHost(), key.getPort());
							ThriftClientKeyPool.this.services = removeFailService(
									ThriftClientKeyPool.this.services, key);
							if (poolConfig.isFailover()) {
								int failovernum = 0;
								while (true) {
									try {
										if (failovernum >= MAXFFILOVERNUM)
											throw new ConnectionFailException("host="
													+ key.getHost() + ", ip="
													+ key.getPort(), e);
										failovernum++;
										TimeUnit.MICROSECONDS.sleep(200);
										transport = getTransport(key);
										log.info(
												"failover to next service host={}, port={}",
												key.getHost(),
												key.getPort());
										transport.open();
										break;
									} catch (TTransportException e1) {
										log.warn(
												"failover fail, services left: host={},port={}",
												key.getHost(),key.getPort());
									}
								}
							}else{
								throw new ConnectionFailException("host="
										+ key.getHost() + ", ip="
										+ key.getPort(), e);
							}
						}
						ThriftClient<T> client = new ThriftClient<T>(
								clientFactory.createClient(protocolFactory
										.makeProtocol(transport)), pool, key);
						if (!ThriftClientKeyPool.this.services.contains(key)) {
							ThriftClientKeyPool.this.services.add(key);
						}
						log.debug("create new object for pool {}", client);
						return client;
					}

					@Override
					public PooledObject<ThriftClient<T>> wrap(
							ThriftClient<T> value) {

						return new DefaultPooledObject<ThriftClient<T>>(value);
					}

					@Override
					public void destroyObject(ServiceInfo key,
							PooledObject<ThriftClient<T>> p) throws Exception {
						p.getObject().close();
						super.destroyObject(key, p);
					}
				}, config);
	}

	public List<ServiceInfo> getServices() {
		return services;
	}

	/**
	 * set new services for this pool
	 *
	 * @param services
	 */
	public void setServices(List<ServiceInfo> services) {
		if (services == null || services.size() == 0) {
			throw new IllegalArgumentException("services is empty!");
		}
		this.services = services;
//		serviceReset = true;
	}

	private TTransport getTransport(ServiceInfo serviceInfo) {

		if (serviceInfo == null) {
			throw new NoBackendServiceException();
		}

		TTransport transport;
		if (poolConfig.getTimeout() > 0) {
			transport = new TSocket(serviceInfo.getHost(),
					serviceInfo.getPort(), poolConfig.getTimeout());
		} else {
			transport = new TSocket(serviceInfo.getHost(),
					serviceInfo.getPort());
		}
		return transport;
	}

	/**
	 * get a random service
	 *
	 * @param serviceList
	 * @return
	 */
	private ServiceInfo getRandomService(List<ServiceInfo> serviceList) {
		if (serviceList == null || serviceList.size() == 0) {
			return null;
		}
		return serviceList.get(RandomUtils.nextInt(0, serviceList.size()));
	}

	private List<ServiceInfo> removeFailService(List<ServiceInfo> list,
			ServiceInfo serviceInfo) {
		log.info("remove service from current service list: host={}, port={}",
				serviceInfo.getHost(), serviceInfo.getPort());
		if (list.contains(serviceInfo)) {
			list.remove(serviceInfo);
		}
		return list;
	}

	/**
	 * get a client from pool
	 *
	 * @return
	 * @throws ThriftException
	 * @throws NoBackendServiceException
	 *             if {@link PoolConfig#setFailover(boolean)} is set and no
	 *             service can connect to
	 * @throws ConnectionFailException
	 *             if {@link PoolConfig#setFailover(boolean)} not set and
	 *             connection fail
	 */
	public ThriftClient<T> getClient(ServiceInfo service)
			throws ThriftException {
		try {
			return pool.borrowObject(service);
		} catch (Exception e) {
			if (e instanceof ThriftException) {
				throw (ThriftException) e;
			}
			throw new ThriftException("Get client from pool failed.", e);
		}
	}

	/**
	 * get a client's IFace from pool
	 * <p/>
	 * <ul>
	 * <li>
	 * <span style="color:red">Important: Iface is totally generated by thrift,
	 * a ClassCastException will be thrown if assign not match!</span></li>
	 * <li>
	 * <span style="color:red">Limitation: The return object can only used
	 * once.</span></li>
	 * </ul>
	 *
	 * @return
	 * @throws ThriftException
	 * @throws NoBackendServiceException
	 *             if {@link PoolConfig#setFailover(boolean)} is set and no
	 *             service can connect to
	 * @throws ConnectionFailException
	 *             if {@link PoolConfig#setFailover(boolean)} not set and
	 *             connection fail
	 * @throws IllegalStateException
	 *             if call method on return object twice
	 */
	@SuppressWarnings("unchecked")
	public <X> X iface(final ServiceInfo service) {
		final ThriftClient<T> client;
		try {
			client = pool.borrowObject(service);
		} catch (Exception e) {
			if (e instanceof ThriftException) {
				throw (ThriftException) e;
			}
			throw new ThriftException("Get client from pool failed.", e);
		}
		final AtomicBoolean returnToPool = new AtomicBoolean(false);
		return (X) Proxy.newProxyInstance(this.getClass().getClassLoader(),
				client.iFace().getClass().getInterfaces(),
				new InvocationHandler() {
					@Override
					public Object invoke(Object proxy, Method method,
							Object[] args) throws Throwable {
						if (returnToPool.get()) {
							throw new IllegalStateException(
									"Object returned via iface can only used once!");
						}
						boolean success = false;
						try {
							Object result = method.invoke(client.iFace(), args);
							success = true;
							return result;
						} finally {
							if (success) {
								pool.returnObject(service, client);
							} else {
								client.closeClient();
								pool.invalidateObject(service, client);
							}
							returnToPool.set(true);
						}
					}
				});
	}

	public <X> X iface() {
		return iface(getRandomService(services));
	}

	@Override
	protected void finalize() throws Throwable {
		if (pool != null) {
			pool.close();
		}
		super.finalize();
	}
}
