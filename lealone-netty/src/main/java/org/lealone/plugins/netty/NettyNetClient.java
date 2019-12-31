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
package org.lealone.plugins.netty;

import java.net.InetSocketAddress;
import java.util.Map;

import org.lealone.db.async.AsyncCallback;
import org.lealone.net.AsyncConnection;
import org.lealone.net.AsyncConnectionManager;
import org.lealone.net.NetClientBase;
import org.lealone.net.NetNode;
import org.lealone.net.TcpClientConnection;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

public class NettyNetClient extends NetClientBase {

    private static final NettyNetClient instance = new NettyNetClient();

    public static NettyNetClient getInstance() {
        return instance;
    }

    private Bootstrap bootstrap;

    private NettyNetClient() {
    }

    @Override
    protected synchronized void openInternal(Map<String, String> config) {
        if (bootstrap == null) {
            EventLoopGroup group = new NioEventLoopGroup();
            bootstrap = new Bootstrap();
            bootstrap.group(group).channel(NioSocketChannel.class).option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            // 延迟到connect成功后再创建连接
                        }
                    });
        }
    }

    @Override
    protected synchronized void closeInternal() {
        if (bootstrap != null) {
            bootstrap.config().group().shutdownGracefully();
            bootstrap = null;
        }
    }

    @Override
    protected void createConnectionInternal(NetNode node, AsyncConnectionManager connectionManager,
            AsyncCallback<AsyncConnection> ac) {
        final InetSocketAddress inetSocketAddress = node.getInetSocketAddress();
        bootstrap.connect(node.getHost(), node.getPort()).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    SocketChannel ch = (SocketChannel) future.channel();
                    NettyWritableChannel writableChannel = new NettyWritableChannel(ch);
                    AsyncConnection conn;
                    if (connectionManager != null) {
                        conn = connectionManager.createConnection(writableChannel, false);
                    } else {
                        conn = new TcpClientConnection(writableChannel, NettyNetClient.this);
                    }
                    ch.pipeline().addLast(new NettyNetClientHandler(NettyNetClient.this, connectionManager, conn));
                    conn.setInetSocketAddress(inetSocketAddress);
                    conn = NettyNetClient.this.addConnection(inetSocketAddress, conn);
                    ac.setAsyncResult(conn);
                } else {
                    ac.setAsyncResult(future.cause());
                }
            }
        });
    }
}
