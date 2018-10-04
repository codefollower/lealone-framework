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

import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.net.NetServerBase;

//TODO 1.支持SSL 2.支持配置参数
public class MinaNetServer extends NetServerBase {

    private static final Logger logger = LoggerFactory.getLogger(MinaNetServer.class);

    private NioSocketAcceptor acceptor;

    @Override
    public synchronized void start() {
        if (isStarted())
            return;
        logger.info("Starting mina net server");
        try {
            acceptor = new NioSocketAcceptor();
            // DefaultIoFilterChainBuilder chain = acceptor.getFilterChain();
            acceptor.setHandler(new MinaNetServerHandler(this));
            acceptor.bind(new InetSocketAddress(getHost(), getPort()));
            super.start();
        } catch (Exception e) {
            checkBindException(e, "Failed to start mina net server");
        }
    }

    @Override
    public synchronized void stop() {
        if (isStopped())
            return;
        logger.info("Stopping mina net server");
        super.stop();
        if (acceptor != null) {
            acceptor.dispose();
            acceptor = null;
        }
    }
}
