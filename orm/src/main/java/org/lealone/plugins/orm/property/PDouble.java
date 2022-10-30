/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.orm.property;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueDouble;
import org.lealone.plugins.orm.Model;
import org.lealone.plugins.orm.format.DoubleFormat;
import org.lealone.plugins.orm.format.JsonFormat;

/**
 * Double property. 
 */
public class PDouble<M extends Model<M>> extends PBaseNumber<M, Double> {

    public PDouble(String name, M model) {
        super(name, model);
    }

    @Override
    protected DoubleFormat getValueFormat(JsonFormat format) {
        return format.getDoubleFormat();
    }

    // 支持Float，避免总是加D后缀
    public final M set(double value) {
        return super.set(value);
    }

    @Override
    protected Value createValue(Double value) {
        return ValueDouble.get(value);
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getDouble();
    }
}
