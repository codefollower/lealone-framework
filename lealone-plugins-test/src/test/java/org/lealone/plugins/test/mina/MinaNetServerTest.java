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
package org.lealone.plugins.test.mina;

import org.lealone.common.exceptions.ConfigException;
import org.lealone.db.Constants;
import org.lealone.p2p.config.Config;
import org.lealone.p2p.config.Config.PluggableEngineDef;
import org.lealone.plugins.mina.MinaNetFactory;
import org.lealone.plugins.postgresql.PgServerEngine;
import org.lealone.server.TcpServerEngine;
import org.lealone.test.start.TcpServerStart;

public class MinaNetServerTest extends TcpServerStart {

    public static void main(String[] args) {
        TcpServerStart.run(MinaNetServerTest.class, args);
    }

    @Override
    public void applyConfig(Config config) throws ConfigException {
        for (PluggableEngineDef e : config.protocol_server_engines) {
            if (TcpServerEngine.NAME.equalsIgnoreCase(e.name)) {
                e.enabled = true;
                e.getParameters().put(Constants.NET_FACTORY_NAME_KEY, MinaNetFactory.NAME);
            } else if (PgServerEngine.NAME.equalsIgnoreCase(e.name)) {
                e.enabled = false;
            }
        }
        super.applyConfig(config);
    }
}
