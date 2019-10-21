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
import java.util.concurrent.CountDownLatch;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.ShutdownHookUtils;
import org.lealone.net.AsyncConnection;
import org.lealone.net.AsyncConnectionManager;
import org.lealone.net.NetClientBase;
import org.lealone.net.NetEndpoint;
import org.lealone.net.TcpClientConnection;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

public class NettyNetClient extends NetClientBase {

    private static final Logger logger = LoggerFactory.getLogger(NettyNetClient.class);

    private static final NettyNetClient instance = new NettyNetClient();

    public static NettyNetClient getInstance() {
        return instance;
    }

    private static NettyNetClient.NettyClient nettyClient;

    private static synchronized void openNettyClient(NettyNetClient nettyNetClient, Map<String, String> config) {
        if (nettyClient == null) {
            nettyClient = new NettyClient(nettyNetClient, config);
        }
    }

    private static synchronized void closeNettyClient() {
        if (nettyClient != null) {
            nettyClient.close();
            nettyClient = null;
        }
    }

    private NettyNetClient() {
    }

    @Override
    protected void openInternal(Map<String, String> config) {
        openNettyClient(this, config);
    }

    @Override
    protected void closeInternal() {
        closeNettyClient();
    }

    @Override
    protected void createConnectionInternal(NetEndpoint endpoint, AsyncConnectionManager connectionManager,
            CountDownLatch latch) throws Exception {
        nettyClient.connect(endpoint, connectionManager, latch);
    }

    private static void removeConnectionInternal(AsyncConnection conn) {
        instance.removeConnection(conn.getInetSocketAddress());
    }

    private static class NettyClient {
        private final NettyNetClient nettyNetClient;
        private final Bootstrap bootstrap;

        public NettyClient(NettyNetClient nettyNetClient, Map<String, String> config) {
            this.nettyNetClient = nettyNetClient;
            EventLoopGroup group = new NioEventLoopGroup();
            bootstrap = new Bootstrap();
            bootstrap.group(group).channel(NioSocketChannel.class).option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            // 延迟到connect成功后再创建连接
                        }
                    });
            ShutdownHookUtils.addShutdownHook(this, () -> {
                close();
            });
        }

        public void close() {
            bootstrap.config().group().shutdownGracefully();
        }

        public void connect(NetEndpoint endpoint, AsyncConnectionManager connectionManager, CountDownLatch latch) {
            final InetSocketAddress inetSocketAddress = endpoint.getInetSocketAddress();
            bootstrap.connect(endpoint.getHost(), endpoint.getPort()).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    try {
                        if (future.isSuccess()) {
                            SocketChannel ch = (SocketChannel) future.channel();
                            NettyWritableChannel channel = new NettyWritableChannel(ch);
                            AsyncConnection conn;
                            if (connectionManager != null) {
                                conn = connectionManager.createConnection(channel, false);
                            } else {
                                conn = new TcpClientConnection(channel, nettyNetClient);
                            }
                            ChannelPipeline p = ch.pipeline();
                            p.addLast(new NettyClientHandler(connectionManager, conn));
                            conn.setInetSocketAddress(inetSocketAddress);
                            nettyNetClient.addConnection(inetSocketAddress, conn);
                        } else {
                            throw DbException.convert(future.cause());
                        }
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
    }

    private static class NettyClientHandler extends ChannelInboundHandlerAdapter {

        private final AsyncConnectionManager connectionManager;
        private final AsyncConnection conn;

        public NettyClientHandler(AsyncConnectionManager connectionManager, AsyncConnection conn) {
            this.connectionManager = connectionManager;
            this.conn = conn;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof ByteBuf) {
                ByteBuf buff = (ByteBuf) msg;
                conn.handle(new NettyBuffer(buff));
                buff.release();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
            String msg = "RemoteAddress " + ctx.channel().remoteAddress();
            logger.error(msg + " exception: " + cause.getMessage(), cause);
            logger.info(msg + " closed");
            if (connectionManager != null)
                connectionManager.removeConnection(conn);
            removeConnectionInternal(conn);
        }
    }
}
