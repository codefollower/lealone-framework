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

import org.lealone.storage.StorageMapCursor;
import org.rocksdb.RocksIterator;

//RocksIterator的行为跟java.util.Iterator不一样，
//java.util.Iterator是先判断hasNext()，如果为true再调用next()取到当前值，
//而RocksIterator是判断isValid()为true时就要把当前值取好了，调用next是转到下一行了。
public class RocksdbMapCursor<K, V> implements StorageMapCursor<K, V> {

    private final RocksdbMap<K, V> map;
    private final RocksIterator iterator;
    private K k;
    private V v;

    public RocksdbMapCursor(RocksdbMap<K, V> map, RocksIterator iterator) {
        this.map = map;
        this.iterator = iterator;
    }

    @Override
    public K getKey() {
        return k;
    }

    @Override
    public V getValue() {
        return v;
    }

    @Override
    public boolean hasNext() {
        boolean isValid = iterator.isValid();
        if (isValid) {
            k = map.k(iterator.key());
            v = map.v(iterator.value());
        } else {
            k = null;
            v = null;
        }
        return isValid;
    }

    @Override
    public K next() {
        iterator.next();
        return k;
    }
}
