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

public class RocksdbStorageMapCursor<K, V> implements Iterator<K>, StorageMapCursor<K, V> {

    public RocksdbStorageMapCursor() {
    }

    @Override
    public K getKey() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public V getValue() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean hasNext() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public K next() {
        // TODO Auto-generated method stub
        return null;
    }

}
