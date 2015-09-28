package com.thrift.client.pool;

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

public interface ThriftProtocolFactory {

    TProtocol makeProtocol(TTransport transport);
}
