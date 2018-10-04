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
package org.lealone.plugins.rocksdb;

import java.util.Map;

import org.lealone.storage.StorageBase;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.StorageDataType;

public class RocksdbStorage extends StorageBase {

    public RocksdbStorage(Map<String, Object> config) {
        super(config);
    }

    @Override
    public <K, V> StorageMap<K, V> openMap(String name, String mapType, StorageDataType keyType,
            StorageDataType valueType, Map<String, String> parameters) {
        RocksdbStorageMap<K, V> map = new RocksdbStorageMap<>(this, (String) config.get("storagePath"), name, keyType,
                valueType);
        maps.put(name, map);
        return map;
    }

}
