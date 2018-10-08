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
package org.lealone.plugins.test;

import java.util.ArrayList;

import org.junit.Test;
import org.lealone.db.Constants;
import org.lealone.net.nio.NioNetFactory;
import org.lealone.p2p.config.Config;
import org.lealone.p2p.config.Config.PluggableEngineDef;
import org.lealone.plugins.mina.MinaNetFactory;
import org.lealone.plugins.mysql.server.MySQLServer;
import org.lealone.plugins.mysql.server.MySQLServerEngine;
import org.lealone.plugins.netty.NettyNetFactory;
import org.lealone.plugins.postgresql.PgServer;
import org.lealone.plugins.postgresql.PgServerEngine;
import org.lealone.server.TcpServerEngine;

public class PluginTestBase extends org.lealone.test.sql.SqlTestBase {

    protected String tableName;

    public PluginTestBase(String tableName) {
        this.tableName = tableName;
    }

    @Test
    public void run() throws Exception {
        executeUpdate("drop table IF EXISTS " + tableName);
        executeUpdate("create table IF NOT EXISTS " + tableName + "(f1 int, f2 int, f3 int)");
        executeUpdate("insert into " + tableName + "(f1, f2, f3) values(1,2,3)");
        executeUpdate("insert into " + tableName + "(f1, f2, f3) values(5,2,3)");
        executeUpdate("insert into " + tableName + "(f1, f2, f3) values(3,2,3)");

        sql = "select count(*) from " + tableName;
        assertEquals(3, getIntValue(1, true));

        int updateCount = executeUpdate("update " + tableName + " set f2=3 where f1>=3");
        assertEquals(2, updateCount);

        sql = "select * from " + tableName;
        printResultSet();
    }

    public static void enableTcpServer(Config config) {
        enableProtocolServer(config, TcpServerEngine.NAME, Constants.DEFAULT_TCP_PORT);
    }

    public static void enablePgServer(Config config) {
        enableProtocolServer(config, PgServerEngine.NAME, PgServer.DEFAULT_PORT);
    }

    public static void enableMySQLServer(Config config) {
        enableProtocolServer(config, MySQLServerEngine.NAME, MySQLServer.DEFAULT_PORT);
    }

    private static void enableProtocolServer(Config config, String protocolServerName, int port) {
        if (config.protocol_server_engines == null) {
            config.protocol_server_engines = new ArrayList<>(1);
        }

        PluggableEngineDef def = new PluggableEngineDef();
        def.enabled = true;
        def.name = protocolServerName;
        def.getParameters().put("port", port + "");

        config.protocol_server_engines.add(def);
    }

    public static void enableMinaNetServer(Config config) {
        enableNetServer(config, MinaNetFactory.NAME);
    }

    public static void enableNettyNetServer(Config config) {
        enableNetServer(config, NettyNetFactory.NAME);
    }

    public static void enableNioNetServer(Config config) {
        enableNetServer(config, NioNetFactory.NAME);
    }

    private static void enableNetServer(Config config, String serverName) {
        for (PluggableEngineDef e : config.protocol_server_engines) {
            e.getParameters().put(Constants.NET_FACTORY_NAME_KEY, serverName);
        }
    }
}
