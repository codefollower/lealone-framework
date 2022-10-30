/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.orm.property;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueInt;
import org.lealone.plugins.orm.Model;
import org.lealone.plugins.orm.format.IntegerFormat;
import org.lealone.plugins.orm.format.JsonFormat;

/**
 * Integer property. 
 */
public class PInteger<M extends Model<M>> extends PBaseNumber<M, Integer> {

    public PInteger(String name, M model) {
        super(name, model);
    }

    @Override
    protected IntegerFormat getValueFormat(JsonFormat format) {
        return format.getIntegerFormat();
    }

    @Override
    protected Value createValue(Integer value) {
        return ValueInt.get(value);
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getInt();
    }
}
