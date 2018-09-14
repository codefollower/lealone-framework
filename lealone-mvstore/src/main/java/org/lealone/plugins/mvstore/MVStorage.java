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
package org.lealone.plugins.mvstore;

import java.util.Map;
import java.util.Set;

import org.lealone.storage.Storage;
import org.lealone.storage.StorageMap;
import org.lealone.storage.type.StorageDataType;

public class MVStorage implements Storage {

    public MVStorage(Map<String, Object> config) {
    }

    @Override
    public <K, V> StorageMap<K, V> openMap(String name, String mapType, StorageDataType keyType,
            StorageDataType valueType, Map<String, String> parameters) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean hasMap(String name) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String nextTemporaryMapName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void backupTo(String fileName) {
        // TODO Auto-generated method stub

    }

    @Override
    public void save() {
        // TODO Auto-generated method stub

    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

    @Override
    public void closeImmediately() {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean isClosed() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Set<String> getMapNames() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public StorageMap<?, ?> getMap(String name) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long getDiskSpaceUsed() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getMemorySpaceUsed() {
        // TODO Auto-generated method stub
        return 0;
    }

}
