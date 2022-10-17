/*
 * Copyright (c) 2011-2019 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */
package org.lealone.plugins.orm.json;

import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A representation of a <a href="http://json.org/">JSON</a> object in Java.
 *
 * Unlike some other languages Java does not have a native understanding of JSON. To enable JSON to be used easily
 * in Vert.x code we use this class to encapsulate the notion of a JSON object.
 *
 * The implementation adheres to the <a href="http://rfc-editor.org/rfc/rfc7493.txt">RFC-7493</a> to support Temporal
 * data types as well as binary data.
 *
 * Please see the documentation for more information.
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 * @author zhh
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class JsonObject extends Json implements Iterable<Map.Entry<String, Object>> {

    private Map<String, Object> map;

    /**
     * Create a new, empty instance
     */
    public JsonObject() {
        map = new LinkedHashMap<>();
    }

    /**
     * Create an instance from a Map. The Map is not copied.
     *
     * @param map the map to create the instance from.
     */
    public JsonObject(Map<String, Object> map) {
        if (map == null) {
            throw new NullPointerException();
        }
        this.map = map;
    }

    /**
     * Create an instance from a string of JSON
     *
     * @param json the string of JSON
     */
    public JsonObject(String json) {
        if (json == null) {
            throw new NullPointerException();
        }
        map = JacksonCodec.decode(json, Map.class);
        if (map == null) {
            throw new DecodeException("Invalid JSON object: " + json);
        }
    }

    /**
     * Get the string value with the specified key, special cases are addressed for extended JSON types {@code Instant},
     * {@code byte[]} and {@code Enum} which can be converted to String.
     *
     * @param key the key to return the value for
     * @return the value string representation or null if no value for that key
     */
    public String getString(String key) {
        Objects.requireNonNull(key);
        return getString(map.get(key));
    }

    /**
     * Get the Integer value with the specified key
     *
     * @param key the key to return the value for
     * @return the value or null if no value for that key
     * @throws java.lang.ClassCastException if the value is not an Integer
     */
    public Integer getInteger(String key) {
        Objects.requireNonNull(key);
        return getInteger(map.get(key));
    }

    /**
     * Get the Long value with the specified key
     *
     * @param key the key to return the value for
     * @return the value or null if no value for that key
     * @throws java.lang.ClassCastException if the value is not a Long
     */
    public Long getLong(String key) {
        Objects.requireNonNull(key);
        return getLong(map.get(key));
    }

    /**
     * Get the Double value with the specified key
     *
     * @param key the key to return the value for
     * @return the value or null if no value for that key
     * @throws java.lang.ClassCastException if the value is not a Double
     */
    public Double getDouble(String key) {
        Objects.requireNonNull(key);
        return getDouble(map.get(key));
    }

    /**
     * Get the Float value with the specified key
     *
     * @param key the key to return the value for
     * @return the value or null if no value for that key
     * @throws java.lang.ClassCastException if the value is not a Float
     */
    public Float getFloat(String key) {
        Objects.requireNonNull(key);
        return getFloat(map.get(key));
    }

    /**
     * Get the Boolean value with the specified key
     *
     * @param key the key to return the value for
     * @return the value or null if no value for that key
     * @throws java.lang.ClassCastException if the value is not a Boolean
     */
    public Boolean getBoolean(String key) {
        Objects.requireNonNull(key);
        return (Boolean) map.get(key);
    }

    /**
     * Get the JsonObject value with the specified key
     *
     * @param key the key to return the value for
     * @return the value or null if no value for that key
     * @throws java.lang.ClassCastException if the value is not a JsonObject
     */
    public JsonObject getJsonObject(String key) {
        Objects.requireNonNull(key);
        Object val = map.get(key);
        if (val instanceof Map) {
            val = new JsonObject((Map) val);
        }
        return (JsonObject) val;
    }

    /**
     * Get the JsonArray value with the specified key
     *
     * @param key the key to return the value for
     * @return the value or null if no value for that key
     * @throws java.lang.ClassCastException if the value is not a JsonArray
     */
    public JsonArray getJsonArray(String key) {
        Objects.requireNonNull(key);
        Object val = map.get(key);
        if (val instanceof List) {
            val = new JsonArray((List) val);
        }
        return (JsonArray) val;
    }

    /**
     * Get the binary value with the specified key.
     *
     * JSON itself has no notion of a binary, this extension complies to the RFC-7493, so this method assumes there is a
     * String value with the key and it contains a Base64 encoded binary, which it decodes if found and returns.
     *
     * @param key the key to return the value for
     * @return the value or null if no value for that key
     * @throws java.lang.ClassCastException       if the value is not a String
     * @throws java.lang.IllegalArgumentException if the String value is not a legal Base64 encoded value
     */
    public byte[] getBinary(String key) {
        Objects.requireNonNull(key);
        return getBinary(map.get(key));
    }

    /**
     * Get the instant value with the specified key.
     *
     * JSON itself has no notion of a temporal types, this extension complies to the RFC-7493, so this method assumes
     * there is a String value with the key and it contains an ISO 8601 encoded date and time format
     * such as "2017-04-03T10:25:41Z", which it decodes if found and returns.
     *
     * @param key the key to return the value for
     * @return the value or null if no value for that key
     * @throws java.lang.ClassCastException            if the value is not a String
     * @throws java.time.format.DateTimeParseException if the String value is not a legal ISO 8601 encoded value
     */
    public Instant getInstant(String key) {
        Objects.requireNonNull(key);
        return getInstant(map.get(key));
    }

    /**
     * Get the value with the specified key, as an Object with types respecting the limitations of JSON.
     * <ul>
     *   <li>{@code Map} will be wrapped to {@code JsonObject}</li>
     *   <li>{@code List} will be wrapped to {@code JsonArray}</li>
     *   <li>{@code Instant} will be converted to {@code String}</li>
     *   <li>{@code byte[]} will be converted to {@code String}</li>
     *   <li>{@code Enum} will be converted to {@code String}</li>
     * </ul>
     *
     * @param key the key to lookup
     * @return the value
     */
    public Object getValue(String key) {
        Objects.requireNonNull(key);
        return wrapJsonValue(map.get(key));
    }

    /**
     * Like {@link #getString(String)} but specifying a default value to return if there is no entry.
     *
     * @param key the key to lookup
     * @param def the default value to use if the entry is not present
     * @return the value or {@code def} if no entry present
     */
    public String getString(String key, String def) {
        String v = getString(key);
        return v != null ? v : def;
    }

    /**
     * Like {@link #getInteger(String)} but specifying a default value to return if there is no entry.
     *
     * @param key the key to lookup
     * @param def the default value to use if the entry is not present
     * @return the value or {@code def} if no entry present
     */
    public Integer getInteger(String key, Integer def) {
        Integer v = getInteger(key);
        return v != null ? v : def;
    }

    /**
     * Like {@link #getLong(String)} but specifying a default value to return if there is no entry.
     *
     * @param key the key to lookup
     * @param def the default value to use if the entry is not present
     * @return the value or {@code def} if no entry present
     */
    public Long getLong(String key, Long def) {
        Long v = getLong(key);
        return v != null ? v : def;
    }

    /**
     * Like {@link #getDouble(String)} but specifying a default value to return if there is no entry.
     *
     * @param key the key to lookup
     * @param def the default value to use if the entry is not present
     * @return the value or {@code def} if no entry present
     */
    public Double getDouble(String key, Double def) {
        Double v = getDouble(key);
        return v != null ? v : def;
    }

    /**
     * Like {@link #getFloat(String)} but specifying a default value to return if there is no entry.
     *
     * @param key the key to lookup
     * @param def the default value to use if the entry is not present
     * @return the value or {@code def} if no entry present
     */
    public Float getFloat(String key, Float def) {
        Float v = getFloat(key);
        return v != null ? v : def;
    }

    /**
     * Like {@link #getBoolean(String)} but specifying a default value to return if there is no entry.
     *
     * @param key the key to lookup
     * @param def the default value to use if the entry is not present
     * @return the value or {@code def} if no entry present
     */
    public Boolean getBoolean(String key, Boolean def) {
        Boolean v = getBoolean(key);
        return v != null ? v : def;
    }

    /**
     * Like {@link #getJsonObject(String)} but specifying a default value to return if there is no entry.
     *
     * @param key the key to lookup
     * @param def the default value to use if the entry is not present
     * @return the value or {@code def} if no entry present
     */
    public JsonObject getJsonObject(String key, JsonObject def) {
        JsonObject v = getJsonObject(key);
        return v != null ? v : def;
    }

    /**
     * Like {@link #getJsonArray(String)} but specifying a default value to return if there is no entry.
     *
     * @param key the key to lookup
     * @param def the default value to use if the entry is not present
     * @return the value or {@code def} if no entry present
     */
    public JsonArray getJsonArray(String key, JsonArray def) {
        JsonArray v = getJsonArray(key);
        return v != null ? v : def;
    }

    /**
     * Like {@link #getBinary(String)} but specifying a default value to return if there is no entry.
     *
     * @param key the key to lookup
     * @param def the default value to use if the entry is not present
     * @return the value or {@code def} if no entry present
     */
    public byte[] getBinary(String key, byte[] def) {
        byte[] v = getBinary(key);
        return v != null ? v : def;
    }

    /**
     * Like {@link #getInstant(String)} but specifying a default value to return if there is no entry.
     *
     * @param key the key to lookup
     * @param def the default value to use if the entry is not present
     * @return the value or {@code def} if no entry present
     */
    public Instant getInstant(String key, Instant def) {
        Instant v = getInstant(key);
        return v != null ? v : def;
    }

    /**
     * Like {@link #getValue(String)} but specifying a default value to return if there is no entry.
     *
     * @param key the key to lookup
     * @param def the default value to use if the entry is not present
     * @return the value or {@code def} if no entry present
     */
    public Object getValue(String key, Object def) {
        Object v = getValue(key);
        return v != null ? v : def;
    }

    /**
     * Does the JSON object contain the specified key?
     *
     * @param key the key
     * @return true if it contains the key, false if not.
     */
    public boolean containsKey(String key) {
        Objects.requireNonNull(key);
        return map.containsKey(key);
    }

    /**
     * Return the set of field names in the JSON objects
     *
     * @return the set of field names
     */
    public Set<String> fieldNames() {
        return map.keySet();
    }

    /**
     * Put a null value into the JSON object with the specified key.
     *
     * @param key  the key
     * @return  a reference to this, so the API can be used fluently
     */
    public JsonObject putNull(String key) {
        Objects.requireNonNull(key);
        map.put(key, null);
        return this;
    }

    /**
     * Put an Object into the JSON object with the specified key.
     *
     * @param key   the key
     * @param value the value
     * @return a reference to this, so the API can be used fluently
     */
    public JsonObject put(String key, Object value) {
        Objects.requireNonNull(key);
        map.put(key, value);
        return this;
    }

    /**
     * Remove an entry from this object.
     *
     * @param key the key
     * @return the value that was removed, or null if none
     */
    public Object remove(String key) {
        Objects.requireNonNull(key);
        return wrapJsonValue(map.remove(key));
    }

    /**
     * Merge in another JSON object.
     *
     * This is the equivalent of putting all the entries of the other JSON object into this object. This is not a deep
     * merge, entries containing (sub) JSON objects will be replaced entirely.
     *
     * @param other the other JSON object
     * @return a reference to this, so the API can be used fluently
     */
    public JsonObject mergeIn(JsonObject other) {
        return mergeIn(other, false);
    }

    /**
     * Merge in another JSON object.
     * A deep merge (recursive) matches (sub) JSON objects in the existing tree and replaces all
     * matching entries. JsonArrays are treated like any other entry, i.e. replaced entirely.
     *
     * @param other the other JSON object
     * @param deep  if true, a deep merge is performed
     * @return a reference to this, so the API can be used fluently
     */
    public JsonObject mergeIn(JsonObject other, boolean deep) {
        return mergeIn(other, deep ? Integer.MAX_VALUE : 1);
    }

    /**
     * Merge in another JSON object.
     * The merge is deep (recursive) to the specified level. If depth is 0, no merge is performed,
     * if depth is greater than the depth of one of the objects, a full deep merge is performed.
     *
     * @param other the other JSON object
     * @param depth depth of merge
     * @return a reference to this, so the API can be used fluently
     */
    public JsonObject mergeIn(JsonObject other, int depth) {
        if (depth < 1) {
            return this;
        }
        if (depth == 1) {
            map.putAll(other.map);
            return this;
        }
        for (Map.Entry<String, Object> e : other.map.entrySet()) {
            if (e.getValue() == null) {
                map.put(e.getKey(), null);
            } else {
                map.merge(e.getKey(), e.getValue(), (oldVal, newVal) -> {
                    if (oldVal instanceof Map) {
                        oldVal = new JsonObject((Map) oldVal);
                    }
                    if (newVal instanceof Map) {
                        newVal = new JsonObject((Map) newVal);
                    }
                    if (oldVal instanceof JsonObject && newVal instanceof JsonObject) {
                        return ((JsonObject) oldVal).mergeIn((JsonObject) newVal, depth - 1);
                    }
                    return newVal;
                });
            }
        }
        return this;
    }

    /**
     * Encode this JSON object as a string.
     *
     * @return the string encoding.
     */
    public String encode() {
        return JacksonCodec.encode(this, false);
    }

    /**
     * Encode this JSON object a a string, with whitespace to make the object easier to read by a human, or other
     * sentient organism.
     *
     * @return the pretty string encoding.
     */
    public String encodePrettily() {
        return JacksonCodec.encode(this, true);
    }

    /**
     * Get the underlying {@code Map} as is.
     *
     * This map may contain values that are not the types returned by the {@code JsonObject}.
     *
     * @return the underlying Map.
     */
    public Map<String, Object> getMap() {
        return map;
    }

    /**
     * Get a stream of the entries in the JSON object.
     *
     * @return a stream of the entries.
     */
    public Stream<Map.Entry<String, Object>> stream() {
        return asStream(iterator());
    }

    /**
     * Get an Iterator of the entries in the JSON object.
     *
     * @return an Iterator of the entries
     */
    @Override
    public Iterator<Map.Entry<String, Object>> iterator() {
        return new Iter(map.entrySet().iterator());
    }

    /**
     * Get the number of entries in the JSON object
     *
     * @return the number of entries
     */
    public int size() {
        return map.size();
    }

    /**
     * Remove all the entries in this JSON object
     */
    public JsonObject clear() {
        map.clear();
        return this;
    }

    /**
     * Is this object entry?
     *
     * @return true if it has zero entries, false if not.
     */
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public String toString() {
        return encode();
    }

    @Override
    public int hashCode() {
        return map.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        // null check
        if (o == null)
            return false;
        // self check
        if (this == o)
            return true;
        // type check and cast
        if (getClass() != o.getClass())
            return false;

        JsonObject other = (JsonObject) o;
        // size check
        if (this.size() != other.size())
            return false;
        // value comparison
        for (String key : map.keySet()) {
            if (!other.containsKey(key)) {
                return false;
            }
            Object thisValue = this.getValue(key);
            Object otherValue = other.getValue(key);
            if (valueEquals(thisValue, otherValue)) {
                continue;
            }
        }
        // all checks passed
        return true;
    }

    private static class Iter implements Iterator<Map.Entry<String, Object>> {

        final Iterator<Map.Entry<String, Object>> mapIter;

        Iter(Iterator<Map.Entry<String, Object>> mapIter) {
            this.mapIter = mapIter;
        }

        @Override
        public boolean hasNext() {
            return mapIter.hasNext();
        }

        @Override
        public Map.Entry<String, Object> next() {
            final Map.Entry<String, Object> entry = mapIter.next();
            final Object val = entry.getValue();
            // perform wrapping
            final Object wrapped = wrapJsonValue(val);
            if (val != wrapped) {
                return new Entry(entry.getKey(), wrapped);
            }
            return entry;
        }

        @Override
        public void remove() {
            mapIter.remove();
        }
    }

    private static class Entry implements Map.Entry<String, Object> {
        final String key;
        final Object value;

        public Entry(String key, Object value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public Object setValue(Object value) {
            throw new UnsupportedOperationException();
        }
    }
}
