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

import java.util.Iterator;

import org.lealone.storage.StorageMapCursor;
import org.rocksdb.RocksIterator;

public class RocksdbStorageMapCursor<K, V> implements Iterator<K>, StorageMapCursor<K, V> {

    private final RocksdbStorageMap<K, V> map;
    private final RocksIterator iterator;

    public RocksdbStorageMapCursor(RocksdbStorageMap<K, V> map, RocksIterator iterator) {
        this.map = map;
        this.iterator = iterator;
    }

    @Override
    public K getKey() {
        return map.k(iterator.key());
    }

    @Override
    public V getValue() {
        return map.v(iterator.value());
    }

    @Override
    public boolean hasNext() {
        return iterator.isValid();
    }

    @Override
    public K next() {
        iterator.next();
        return getKey();
    }
}
