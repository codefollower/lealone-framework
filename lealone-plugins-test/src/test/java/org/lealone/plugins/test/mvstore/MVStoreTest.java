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
package org.lealone.plugins.test.mvstore;

import org.junit.Test;
import org.lealone.plugins.mvstore.MVStorageEngine;

public class MVStoreTest extends org.lealone.test.sql.SqlTestBase {

    public MVStoreTest() {
        initTransactionEngine();
        setStorageEngineName(MVStorageEngine.NAME);
        setEmbedded(true);
        printURL();
    }

    @Test
    public void run() throws Exception {
        stmt.executeUpdate("drop table IF EXISTS MinaNetClientTest");
        stmt.executeUpdate("create table IF NOT EXISTS MinaNetClientTest(f1 int, f2 int, f3 int)");
        stmt.executeUpdate("insert into MinaNetClientTest(f1, f2, f3) values(1,2,3)");
        stmt.executeUpdate("insert into MinaNetClientTest(f1, f2, f3) values(5,2,3)");
        stmt.executeUpdate("insert into MinaNetClientTest(f1, f2, f3) values(3,2,3)");

        sql = "select count(*) from MinaNetClientTest";
        assertEquals(3, getIntValue(1, true));

        sql = "select * from MinaNetClientTest";
        printResultSet();
    }
}
