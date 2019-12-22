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

import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.net.AsyncConnection;
import org.lealone.net.AsyncConnectionManager;
import org.lealone.net.NetClientBase;
import org.lealone.net.NetNode;
import org.lealone.net.TcpClientConnection;

public class MinaNetClient extends NetClientBase {

    private static final MinaNetClient instance = new MinaNetClient();

    public static MinaNetClient getInstance() {
        return instance;
    }

    private NioSocketConnector connector;

    private MinaNetClient() {
    }

    @Override
    protected synchronized void openInternal(Map<String, String> config) {
        if (connector == null) {
            connector = new NioSocketConnector();
            connector.setHandler(new MinaNetClientHandler(this));
        }
    }

    @Override
    protected synchronized void closeInternal() {
        if (connector != null) {
            connector.dispose();
            connector = null;
        }
    }

    @Override
    protected void createConnectionInternal(NetNode node, AsyncConnectionManager connectionManager,
            AsyncHandler<AsyncResult<AsyncConnection>> asyncHandler) {
        InetSocketAddress inetSocketAddress = node.getInetSocketAddress();
        ConnectFuture future = connector.connect(inetSocketAddress);
        future.addListener(f -> {
            if (f.isDone()) {
                IoSession session = future.getSession();
                MinaWritableChannel writableChannel = new MinaWritableChannel(session);
                AsyncConnection conn;
                if (connectionManager != null) {
                    conn = connectionManager.createConnection(writableChannel, false);
                } else {
                    conn = new TcpClientConnection(writableChannel, this);
                }
                conn.setInetSocketAddress(inetSocketAddress);
                addConnection(inetSocketAddress, conn);
                asyncHandler.handle(new AsyncResult<>(conn));
            }
        });
    }

    AsyncConnection getConnection(IoSession session) {
        return getConnection(getInetSocketAddress(session));
    }

    void removeConnection(IoSession session) {
        removeConnection(getInetSocketAddress(session));
    }

    private InetSocketAddress getInetSocketAddress(IoSession session) {
        return (InetSocketAddress) session.getRemoteAddress();
    }
}
