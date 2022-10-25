/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.orm.property;

import java.util.Map;

import org.lealone.db.value.Value;
import org.lealone.plugins.orm.Model;
import org.lealone.plugins.orm.Model.CaseFormat;
import org.lealone.plugins.orm.ModelProperty;

public abstract class PBase<M extends Model<M>, T> extends ModelProperty<M> {

    protected T value;

    public PBase(String name, M model) {
        super(name, model);
    }

    protected abstract Value createValue(T value);

    private PBase<M, T> p(M model) {
        return this.<PBase<M, T>> getModelProperty(model);
    }

    public final M set(T value) {
        M m = getModel();
        if (m != model) {
            return p(m).set(value);
        }
        if (!areEqual(this.value, value)) {
            this.value = value;
            expr().set(name, createValue(value));
        }
        return model;
    }

    public T get() {
        M m = getModel();
        if (m != model) {
            return p(m).get();
        }
        return value;
    }

    // 把value编码为json
    protected Object encodeValue() {
        return value;
    }

    @Override
    protected final void serialize(Map<String, Object> map, CaseFormat format) {
        if (value != null) {
            map.put(getName(format), encodeValue());
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void deserialize(Object v) {
        value = (T) v;
    }

    @Override
    protected void deserializeAndSet(Object v) {
        deserialize(v);
        expr().set(name, createValue(value));
    }
}
