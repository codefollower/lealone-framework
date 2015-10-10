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
package org.lealone.wiredtiger;

import java.io.File;
import java.util.HashMap;

import org.lealone.db.table.MVTable;
import org.lealone.db.table.Table;
import org.lealone.mvstore.MVStorageEngine;
import org.lealone.storage.CreateTableData;
import org.lealone.storage.Database;
import org.lealone.storage.StorageEngineManager;
import org.lealone.storage.TransactionStorageEngine;

import com.wiredtiger.db.Connection;
import com.wiredtiger.db.wiredtiger;

/**
 * A storage engine that internally uses the WiredTiger.
 */
public class WTStorageEngine extends MVStorageEngine implements TransactionStorageEngine {
    public static final String NAME = "WT";
    private static final HashMap<String, Connection> connections = new HashMap<>(1);

    // 见StorageEngineManager.StorageEngineService中的注释
    public WTStorageEngine() {
        StorageEngineManager.registerStorageEngine(this);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public synchronized Table createTable(CreateTableData data0) {
        org.lealone.db.table.CreateTableData data = (org.lealone.db.table.CreateTableData) data0;
        org.lealone.db.Database db = data.session.getDatabase();
        String dbName = db.getName();
        Connection conn = connections.get(dbName);
        if (conn == null) {
            conn = createConnection(dbName);
            connections.put(dbName, conn);

            init(this, db);
        }
        MVTable table = new MVTable(data, this);
        table.init(data.session);
        return table;
    }

    @Override
    public synchronized WTMapBuilder createStorageMapBuilder(String dbName) {
        return new WTMapBuilder(connections.get(dbName).open_session(null));
    }

    @Override
    public synchronized void close(Database db0) {
        org.lealone.db.Database db = (org.lealone.db.Database) db0;
        super.close(db);
        Connection conn = connections.remove(db.getName());
        if (conn != null) {
            conn.close(null);
        }
    }

    private Connection createConnection(String dbName) {
        File home = new File(dbName);
        if (!home.exists())
            home.mkdir();

        return wiredtiger.open(dbName, "create");
    }

    @Override
    public void flush(Database db0) {
        org.lealone.db.Database db = (org.lealone.db.Database) db0;
        connections.get(db.getName()).async_flush();
    }

    @Override
    public void sync(Database db) {
        flush(db);
    }
}
