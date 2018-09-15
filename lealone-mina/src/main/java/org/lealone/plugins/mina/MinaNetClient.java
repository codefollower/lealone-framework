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
import java.util.Properties;

import org.lealone.net.AsyncConnection;
import org.lealone.net.AsyncConnectionManager;
import org.lealone.net.NetEndpoint;

public class MinaNetClient implements org.lealone.net.NetClient {

    private static final MinaNetClient instance = new MinaNetClient();

    public static MinaNetClient getInstance() {
        return instance;
    }

    @Override
    public AsyncConnection createConnection(Properties prop, NetEndpoint endpoint) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AsyncConnection createConnection(Properties prop, NetEndpoint endpoint,
            AsyncConnectionManager connectionManager) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void removeConnection(InetSocketAddress inetSocketAddress, boolean closeClient) {
        // TODO Auto-generated method stub

    }

}