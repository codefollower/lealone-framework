/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.orm.property;

import java.util.Map;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueMap;
import org.lealone.plugins.orm.Model;
import org.lealone.plugins.orm.format.JsonFormat;
import org.lealone.plugins.orm.format.MapFormat;
import org.lealone.plugins.orm.json.Json;

/**
 * Map property.
 */
public class PMap<M extends Model<M>, K, V> extends PBase<M, Map<K, V>> {

    private final Class<?> keyClass;

    public PMap(String name, M model, Class<?> keyClass) {
        super(name, model);
        this.keyClass = keyClass;
    }

    public Class<?> getKeyClass() {
        return keyClass;
    }

    @Override
    protected MapFormat<K, V> getValueFormat(JsonFormat format) {
        return format.getMapFormat();
    }

    @Override
    protected Value createValue(Map<K, V> values) {
        return ValueMap.get(values);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void deserialize(Value v) {
        if (v instanceof ValueMap) {
            this.value = (Map<K, V>) v.getObject();
        }
    }

    @Override
    protected void decodeAndSet(Object v, JsonFormat format) {
        v = Json.convertToMap(v, keyClass);
        super.decodeAndSet(v, format);
    }
}
