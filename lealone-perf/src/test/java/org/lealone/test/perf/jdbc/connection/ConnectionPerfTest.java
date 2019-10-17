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
package org.lealone.test.perf.jdbc.connection;

import java.sql.Connection;

import org.lealone.test.perf.PerfTestBase;

//测试创建jdbc connection的性能
public abstract class ConnectionPerfTest extends PerfTestBase {

    private final int connectionCount = 1000;

    protected abstract Connection getConnection() throws Exception;

    @Override
    public void run(int loop) throws Exception {
        Connection[] connections = new Connection[connectionCount];

        long t1 = System.currentTimeMillis();
        for (int i = 0; i < connectionCount; i++) {
            connections[i] = getConnection();
        }
        long t2 = System.currentTimeMillis();

        printResult(loop, ", create connection count: " + connectionCount + ", total time: " + (t2 - t1) + " ms"
                + ", avg time: " + (t2 - t1) / (connectionCount * 1.0) + " ms");

        for (int i = 0; i < connectionCount; i++) {
            connections[i].close();
        }
    }
}
