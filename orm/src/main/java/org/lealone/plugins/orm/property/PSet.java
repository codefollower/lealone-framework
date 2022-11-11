/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.orm.property;

import java.util.Set;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueSet;
import org.lealone.plugins.orm.Model;
import org.lealone.plugins.orm.format.JsonFormat;
import org.lealone.plugins.orm.format.SetFormat;

/**
 * Set property.
 */
public class PSet<M extends Model<M>, E> extends PBase<M, Set<E>> {

    public PSet(String name, M model) {
        super(name, model);
    }

    @Override
    protected SetFormat<E> getValueFormat(JsonFormat format) {
        return format.getSetFormat();
    }

    @Override
    protected Value createValue(Set<E> values) {
        return ValueSet.get(values);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void deserialize(Value v) {
        if (v instanceof ValueSet) {
            this.value = (Set<E>) v.getObject();
        }
    }
}
