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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.lealone.common.util.BitField;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.DataType;

import com.wiredtiger.db.Connection;
import com.wiredtiger.db.wiredtiger;

public class WTStorage implements Storage {
    private final Connection conn;
    private final ConcurrentHashMap<String, StorageMap<?, ?>> maps = new ConcurrentHashMap<>();

    private int lastMapId;

    public WTStorage(Map<String, Object> config) {
        String storageName = (String) config.get("storageName");
        conn = createConnection(storageName);
    }

    private Connection createConnection(String dbName) {
        File home = new File(dbName);
        if (!home.exists())
            home.mkdir();

        return wiredtiger.open(dbName, "create");
    }

    @Override
    public <K, V> StorageMap<K, V> openMap(String name, String mapType, DataType keyType, DataType valueType,
            Map<String, String> parameters) {
        WTMap<K, V> map = new WTMap<>(conn.open_session(null), name, keyType, valueType);
        maps.put(name, map);
        return map;
    }

    @Override
    public boolean hasMap(String name) {
        return maps.containsKey(name);
    }

    @Override
    public void removeTemporaryMaps(BitField objectIds) {
        // TODO Auto-generated method stub
    }

    @Override
    public synchronized String nextTemporaryMapName() {
        return "temp." + lastMapId++;
    }

    @Override
    public void backupTo(String fileName) {
        // TODO Auto-generated method stub
    }

    @Override
    public void flush() {
        conn.async_flush();
    }

    @Override
    public void sync() {
        flush();
    }

    @Override
    public void close() {
        conn.close(null);
    }

    @Override
    public void closeImmediately() {
        close();
    }

}
