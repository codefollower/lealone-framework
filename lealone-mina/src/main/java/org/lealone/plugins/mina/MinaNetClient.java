/*
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
package org.lealone.plugins.mina;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.lealone.common.exceptions.DbException;
import org.lealone.net.AsyncConnection;
import org.lealone.net.AsyncConnectionManager;
import org.lealone.net.NetEndpoint;
import org.lealone.net.TcpConnection;

public class MinaNetClient implements org.lealone.net.NetClient {

    private static final ConcurrentHashMap<InetSocketAddress, AsyncConnection> asyncConnections = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<IoSession, AsyncConnection> sessionToConnectionMap = new ConcurrentHashMap<>();
    private static final MinaNetClient instance = new MinaNetClient();

    public static MinaNetClient getInstance() {
        return instance;
    }

    private final NioSocketConnector connector;

    private MinaNetClient() {
        connector = new NioSocketConnector();
        connector.setHandler(new MinaNetClientHandler(this));
    }

    @Override
    public AsyncConnection createConnection(Map<String, String> config, NetEndpoint endpoint) {
        return createConnection(config, endpoint, null);
    }

    @Override
    public AsyncConnection createConnection(Map<String, String> config, NetEndpoint endpoint,
            AsyncConnectionManager connectionManager) {
        InetSocketAddress inetSocketAddress = endpoint.getInetSocketAddress();
        AsyncConnection conn = asyncConnections.get(inetSocketAddress);
        if (conn == null) {
            synchronized (MinaNetClient.class) {
                conn = asyncConnections.get(inetSocketAddress);
                if (conn == null) {
                    ConnectFuture future = connector.connect(inetSocketAddress);
                    future.awaitUninterruptibly();
                    if (!future.isConnected()) {
                        throw new RuntimeException("Failed to connect " + inetSocketAddress, future.getException());
                    }
                    IoSession session = future.getSession();
                    MinaWritableChannel writableChannel = new MinaWritableChannel(session);
                    try {
                        if (connectionManager != null) {
                            conn = connectionManager.createConnection(writableChannel, false);
                        } else {
                            conn = new TcpConnection(writableChannel, this);
                        }
                        conn.setInetSocketAddress(inetSocketAddress);
                        asyncConnections.put(inetSocketAddress, conn);
                        sessionToConnectionMap.put(session, conn);
                    } catch (Exception e) {
                        throw DbException.convert(e);
                    }
                }
            }
        }
        return conn;
    }

    @Override
    public void removeConnection(InetSocketAddress inetSocketAddress, boolean closeClient) {
        asyncConnections.remove(inetSocketAddress);
        if (closeClient && asyncConnections.isEmpty()) {
            connector.dispose();
        }
    }

    AsyncConnection getConnection(IoSession session) {
        return sessionToConnectionMap.get(session);
    }

    void removeConnection(IoSession session) {
        AsyncConnection conn = sessionToConnectionMap.remove(session);
        if (conn != null)
            removeConnection(conn.getInetSocketAddress(), false);
    }
}
