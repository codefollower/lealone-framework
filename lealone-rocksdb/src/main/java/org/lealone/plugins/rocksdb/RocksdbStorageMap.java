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

import org.lealone.db.value.ValueLong;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageMapBase;
import org.lealone.storage.StorageMapCursor;
import org.lealone.storage.type.StorageDataType;

public class RocksdbStorageMap<K, V> extends StorageMapBase<K, V> {

    private final Storage storage;

    public RocksdbStorageMap(Storage storage, String name, StorageDataType keyType, StorageDataType valueType) {
        super(name, keyType, valueType);
        this.storage = storage;
        setLastKey(lastKey());
    }

    @Override
    public Storage getStorage() {
        return storage;
    }

    @Override
    public K append(V value) {
        @SuppressWarnings("unchecked")
        K key = (K) ValueLong.get(lastKey.incrementAndGet());
        put(key, value);
        return key;
    }

    @Override
    public V get(K key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public V put(K key, V value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public V putIfAbsent(K key, V value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public V remove(K key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public K firstKey() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public K lastKey() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public K lowerKey(K key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public K floorKey(K key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public K higherKey(K key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public K ceilingKey(K key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean areValuesEqual(Object a, Object b) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int size() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long sizeAsLong() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean containsKey(K key) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isEmpty() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isInMemory() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public StorageMapCursor<K, V> cursor(K from) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void clear() {
        // TODO Auto-generated method stub

    }

    @Override
    public void remove() {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean isClosed() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

    @Override
    public void save() {
        // TODO Auto-generated method stub

    }
}
