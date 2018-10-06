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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.DataBuffer;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageMapBase;
import org.lealone.storage.StorageMapCursor;
import org.lealone.storage.fs.FileUtils;
import org.lealone.storage.type.StorageDataType;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

public class RocksdbStorageMap<K, V> extends StorageMapBase<K, V> {

    private final RocksDB db;
    private final String dbPath;
    private boolean closed;

    public RocksdbStorageMap(RocksdbStorage storage, String storageDir, String name, StorageDataType keyType,
            StorageDataType valueType) {
        super(name, keyType, valueType, storage);

        Options options = new Options();
        options.setCreateIfMissing(true);
        BlockBasedTableConfig config = new BlockBasedTableConfig();
        options.setTableFormatConfig(config);
        dbPath = storageDir + File.separator + name;
        try {
            db = RocksDB.open(options, dbPath);
        } catch (RocksDBException e) {
            throw ioe(e, "Failed to open " + dbPath);
        }
        setMaxKey(lastKey());
    }

    private static DbException ioe(Throwable e, String msg) {
        throw DbException.convertIOException(new IOException(e), msg);
    }

    private static byte[] toBytes(Object obj, StorageDataType type) {
        try (DataBuffer buff = DataBuffer.create()) {
            type.write(buff, obj);
            ByteBuffer b = buff.getAndFlipBuffer();
            byte[] dest = new byte[b.limit()];
            System.arraycopy(b.array(), b.arrayOffset(), dest, 0, dest.length);
            return dest;
        }
    }

    private byte[] k(Object key) {
        return toBytes(key, keyType);
    }

    private byte[] v(Object value) {
        return toBytes(value, valueType);
    }

    @SuppressWarnings("unchecked")
    K k(byte[] key) {
        return (K) keyType.read(ByteBuffer.wrap(key));
    }

    @SuppressWarnings("unchecked")
    V v(byte[] value) {
        if (value == null)
            return null;
        return (V) valueType.read(ByteBuffer.wrap(value));
    }

    @Override
    public Storage getStorage() {
        return storage;
    }

    @Override
    public V get(K key) {
        byte[] value;
        try {
            value = db.get(k(key));
        } catch (RocksDBException e) {
            throw ioe(e, "Failed to get " + key);
        }
        return v(value);
    }

    @Override
    public V put(K key, V value) {
        V old = get(key);
        try {
            db.put(k(key), v(value));
        } catch (RocksDBException e) {
            throw ioe(e, "Failed to put " + key);
        }
        return old;
    }

    @Override
    public V putIfAbsent(K key, V value) {
        if (get(key) != null)
            return put(key, value);
        return null;
    }

    @Override
    public V remove(K key) {
        V old = get(key);
        try {
            db.delete(k(key));
        } catch (RocksDBException e) {
            throw ioe(e, "Failed to remove " + key);
        }
        return old;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        V old = get(key);
        if (areValuesEqual(old, oldValue)) {
            put(key, newValue);
            return true;
        }
        return false;
    }

    @Override
    public K firstKey() {
        try (RocksIterator iterator = db.newIterator()) {
            iterator.seekToFirst();
            if (iterator.isValid()) {
                return k(iterator.key());
            }
        }
        return null;
    }

    @Override
    public K lastKey() {
        try (RocksIterator iterator = db.newIterator()) {
            iterator.seekToLast();
            if (iterator.isValid()) {
                return k(iterator.key());
            }
        }
        return null;
    }

    @Override
    public K lowerKey(K key) { // 小于给定key的最大key
        return getMinMax(key, true, true);
    }

    @Override
    public K floorKey(K key) { // 小于或等于给定key的最大key
        return getMinMax(key, true, false);
    }

    @Override
    public K higherKey(K key) { // 大于给定key的最小key
        return getMinMax(key, false, true);
    }

    @Override
    public K ceilingKey(K key) { // 大于或等于给定key的最小key
        return getMinMax(key, false, false);
    }

    private K getMinMax(K key, boolean min, boolean excluding) {
        try (RocksIterator iterator = db.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                K k = k(iterator.key());
                if (keyType.compare(key, k) >= 0) {
                    if (min) {
                        if (excluding) {
                            iterator.prev();
                            if (iterator.isValid())
                                return k(iterator.key());
                            else
                                return null;
                        } else {
                            return k;
                        }
                    } else {
                        if (excluding) {
                            iterator.next();
                            if (iterator.isValid())
                                return k(iterator.key());
                            else
                                return null;
                        } else {
                            return k;
                        }
                    }
                }
            }
        }
        return null;
    }

    @Override
    public boolean areValuesEqual(Object a, Object b) {
        return areEqual(a, b, valueType);
    }

    private static boolean areEqual(Object a, Object b, StorageDataType dataType) {
        if (a == b) {
            return true;
        } else if (a == null || b == null) {
            return false;
        }
        return dataType.compare(a, b) == 0;
    }

    @Override
    public int size() {
        return (int) sizeAsLong();
    }

    @Override
    public long sizeAsLong() {
        long size = 0;
        try (final RocksIterator iterator = db.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                size++;
            }
        }
        return size;
    }

    @Override
    public boolean containsKey(K key) {
        return get(key) != null;
    }

    @Override
    public boolean isEmpty() {
        try (RocksIterator iterator = db.newIterator()) {
            iterator.seekToFirst();
            if (iterator.isValid()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isInMemory() {
        return false;
    }

    @Override
    public StorageMapCursor<K, V> cursor(K from) {
        RocksIterator iterator = db.newIterator();
        if (from == null) {
            iterator.seekToFirst();
            return new RocksdbStorageMapCursor<>(this, iterator);
        }
        for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
            K key = k(iterator.key());
            if (keyType.compare(key, from) >= 0) {
                break;
            }
        }
        return new RocksdbStorageMapCursor<>(this, iterator);
    }

    @Override
    public void clear() {
        remove();
    }

    @Override
    public void remove() {
        close();
        storage.closeMap(getName());
        FileUtils.deleteRecursive(dbPath, true);
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        db.close();
        closed = true;
    }

    @Override
    public void save() {
    }
}
