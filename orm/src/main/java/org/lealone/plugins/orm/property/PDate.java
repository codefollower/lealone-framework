/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.orm.property;

import java.sql.Date;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueDate;
import org.lealone.plugins.orm.Model;
import org.lealone.plugins.orm.format.DateFormat;
import org.lealone.plugins.orm.format.JsonFormat;

/**
 * Java sql date property. 
 */
public class PDate<M extends Model<M>> extends PBaseDate<M, Date> {

    public PDate(String name, M model) {
        super(name, model);
    }

    @Override
    protected DateFormat getValueFormat(JsonFormat format) {
        return format.getDateFormat();
    }

    public M set(String value) {
        return set(Date.valueOf(value.toString()));
    }

    @Override
    protected Value createValue(Date value) {
        return ValueDate.get(value);
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getDate();
    }
}
