/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.orm.property;

import java.sql.Time;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueTime;
import org.lealone.plugins.orm.Model;
import org.lealone.plugins.orm.format.JsonFormat;
import org.lealone.plugins.orm.format.TimeFormat;

/**
 * Time property.
 */
public class PTime<M extends Model<M>> extends PBaseNumber<M, Time> {

    public PTime(String name, M model) {
        super(name, model);
    }

    @Override
    protected TimeFormat getValueFormat(JsonFormat format) {
        return format.getTimeFormat();
    }

    @Override
    protected Value createValue(Time value) {
        return ValueTime.get(value);
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getTime();
    }
}
