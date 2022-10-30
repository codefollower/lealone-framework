/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.orm.property;

import java.sql.Timestamp;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueTimestamp;
import org.lealone.plugins.orm.Model;
import org.lealone.plugins.orm.format.JsonFormat;
import org.lealone.plugins.orm.format.TimestampFormat;

/**
 * Property for java sql Timestamp.
 */
public class PTimestamp<M extends Model<M>> extends PBaseDate<M, Timestamp> {

    public PTimestamp(String name, M model) {
        super(name, model);
    }

    @Override
    protected TimestampFormat getValueFormat(JsonFormat format) {
        return format.getTimestampFormat();
    }

    @Override
    protected Value createValue(Timestamp value) {
        return ValueTimestamp.get(value);
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getTimestamp();
    }
}
