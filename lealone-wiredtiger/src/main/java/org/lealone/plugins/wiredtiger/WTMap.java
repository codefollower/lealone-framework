/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.lealone.plugins.wiredtiger;

import java.nio.ByteBuffer;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.storage.StorageMapBase;
import org.lealone.storage.type.StorageDataType;

import com.wiredtiger.db.Cursor;
import com.wiredtiger.db.SearchStatus;
import com.wiredtiger.db.Session;

@SuppressWarnings("unchecked")
public class WTMap<K, V> extends StorageMapBase<K, V> {

    private final Session wtSession;
    private final int id;
    private Cursor wtCursor;

    private DataBuffer writeBuffer;
    private boolean closed;

    public WTMap(String name, WTStorage storage, Session wtSession) {
        this(name, null, null, storage, wtSession);
    }

    public WTMap(String name, StorageDataType keyType, StorageDataType valueType, WTStorage storage,
            Session wtSession) {
        super(name, keyType, valueType, storage);
        this.wtSession = wtSession;

        id = getMapId(wtSession, name);

        wtSession.create("table:" + name, "key_format=u,value_format=u");

        openWTCursor();
        setMaxKey(lastKey());
    }

    private void openWTCursor() {
        wtCursor = wtSession.open_cursor("table:" + name, null, "append");
    }

    private static int getMapId(Session wtSession, String name) {
        wtSession.create("table:lealone_map_id", "key_format=S,value_format=i");
        Cursor wtCursor = wtSession.open_cursor("table:lealone_map_id", null, "append");

        int id;
        name += "_map_id";
        wtCursor.putKeyString(name);
        if (wtCursor.search() == 0) {
            id = wtCursor.getValueInt();
        } else {
            wtCursor.putKeyString("max_id");
            if (wtCursor.search() == 0) {
                id = wtCursor.getValueInt();
                wtCursor.putKeyString("max_id");
                wtCursor.putValueInt(id + 1);
                wtCursor.update();
            } else {
                id = 1;
                wtCursor.putKeyString("max_id");
                wtCursor.putValueInt(id + 1);
                wtCursor.insert();
            }
            wtCursor.putKeyString(name);
            wtCursor.putValueInt(id);
            wtCursor.insert();
        }
        wtCursor.close();

        return id;
    }

    private DataBuffer getDataBuffer() {
        DataBuffer buff;
        if (writeBuffer != null) {
            buff = writeBuffer;
            buff.clear();
        } else {
            buff = DataBuffer.create();
        }
        return buff;
    }

    void putWTKey(Object key) {
        DataBuffer buff = getDataBuffer();
        keyType.write(buff, key);
        ByteBuffer b = buff.getBuffer();
        b.flip();
        wtCursor.putKeyByteArray(b.array(), b.arrayOffset(), b.limit());
    }

    private void putWTValue(Object value) {
        DataBuffer buff = getDataBuffer();
        valueType.write(buff, value);
        ByteBuffer b = buff.getBuffer();
        b.flip();
        wtCursor.putValueByteArray(b.array(), b.arrayOffset(), b.limit());
    }

    Object getWTKey() {
        byte[] buff = wtCursor.getKeyByteArray();
        if (buff != null)
            return keyType.read(ByteBuffer.wrap(buff));
        return null;
    }

    Object getWTValue() {
        byte[] buff = wtCursor.getValueByteArray();
        if (buff != null)
            return valueType.read(ByteBuffer.wrap(buff));
        return null;
    }

    public int getId() {
        return id;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public V get(Object key) {
        putWTKey(key);
        if (wtCursor.search() == 0)
            return (V) getWTValue();
        return null;
    }

    @Override
    public V put(K key, V value) {
        V old = get(key);

        putWTKey(key);
        putWTValue(value);
        wtCursor.insert();

        return old;
    }

    @Override
    public V putIfAbsent(K key, V value) {
        V old = get(key);
        if (old == null) {
            put(key, value);
        }
        return old;
    }

    @Override
    public V remove(Object key) {
        V old = get(key);
        putWTKey(key);
        wtCursor.remove();
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
    public boolean containsKey(Object key) {
        putWTKey(key);
        return (wtCursor.next() == 0);
    }

    @Override
    public boolean isEmpty() {
        wtCursor.reset();
        return (wtCursor.next() != 0); // 不等于0时表示没有记录
    }

    @Override
    public long size() {
        wtCursor.reset();
        long size = 0;
        while (wtCursor.next() == 0)
            size++;
        return size;
    }

    @Override
    public void clear() {
        wtCursor.close();
        wtSession.truncate("table:" + name, null, null, null);
        openWTCursor();
    }

    @Override
    public void remove() {
        wtCursor.close();
        wtSession.drop("table:" + name, null);
        wtSession.close(null);
        closed = true;
    }

    @Override
    public K firstKey() {
        wtCursor.reset();
        if (wtCursor.next() == 0)
            return (K) getWTKey();
        return null;
    }

    @Override
    public K lastKey() {
        wtCursor.reset();
        if (wtCursor.prev() == 0)
            return (K) getWTKey();
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
        wtCursor.reset();
        putWTKey(key);

        SearchStatus exact = wtCursor.search_near();

        if (min) { // 小于或等于给定key的最大key
            if (exact == SearchStatus.SMALLER) {
                return (K) getWTKey();
            } else if (exact == SearchStatus.LARGER) {
                if ((wtCursor.prev() == 0)) // 继续找上一个
                    return (K) getWTKey();
                return null;
            } else if (exact == SearchStatus.FOUND) {
                if (!excluding || (wtCursor.prev() == 0)) // 继续找上一个
                    return (K) getWTKey();
                return null;
            }

            return null;
        } else { // 大于或等于给定key的最小key
            if (exact == SearchStatus.SMALLER) {
                if ((wtCursor.next() == 0)) // 继续找下一个
                    return (K) getWTKey();
                return null;
            } else if (exact == SearchStatus.LARGER) {
                return (K) getWTKey();
            } else if (exact == SearchStatus.FOUND) {
                if (!excluding || (wtCursor.next() == 0)) // 继续找下一个
                    return (K) getWTKey();
                return null;
            }

            return null;
        }
    }

    public long getKeyIndex(K key) {
        long index = -1;
        wtCursor.reset();
        while (wtCursor.next() == 0) {
            index++;
            if (areEqual(key, getWTKey(), keyType))
                break;
        }
        return index;
    }

    public K getKey(long index) {
        if (index < 0)
            return null;

        wtCursor.reset();
        long i = 0;
        while (wtCursor.next() == 0) {
            if (i == index)
                break;

            i++;
        }
        if (index != i)
            return null;

        return (K) getWTKey();
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
    public WTMapCursor<K, V> cursor(K from) {
        return new WTMapCursor<K, V>(wtCursor, this, from);
    }

    public Set<Entry<K, V>> entrySet() {
        final WTMap<K, V> map = this;
        return new AbstractSet<Entry<K, V>>() {
            @Override
            public Iterator<Entry<K, V>> iterator() {
                final WTMapCursor<K, V> cursor = new WTMapCursor<K, V>(wtCursor, map, null);
                return new Iterator<Entry<K, V>>() {
                    @Override
                    public boolean hasNext() {
                        return cursor.hasNext();
                    }

                    @Override
                    public Entry<K, V> next() {
                        K k = cursor.next();
                        return new MapEntry<K, V>(k, cursor.getValue());
                    }

                    @Override
                    public void remove() {
                        throw DataUtils.newUnsupportedOperationException("Removing is not supported");
                    }
                };
            }

            @Override
            public int size() {
                return (int) WTMap.this.size();
            }

            @Override
            public boolean contains(Object o) {
                return WTMap.this.containsKey(o);
            }
        };
    }

    @Override
    public boolean isInMemory() {
        return false;
    }

    @Override
    public void save() {
    }

    @Override
    public void close() {
        wtSession.close(null);
    }

    /**
     * An entry of a map.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    public static class MapEntry<K, V> implements Map.Entry<K, V> {

        private final K key;
        private final V value;

        public MapEntry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(V value) {
            throw DataUtils.newUnsupportedOperationException("Updating the value is not supported");
        }
    }
}
