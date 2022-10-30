/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.orm.property;

import java.sql.Clob;
import java.sql.SQLException;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueString;
import org.lealone.plugins.orm.Model;
import org.lealone.plugins.orm.format.ClobFormat;
import org.lealone.plugins.orm.format.JsonFormat;

public class PClob<M extends Model<M>> extends PBase<M, Clob> {

    public PClob(String name, M model) {
        super(name, model);
    }

    public static String toString(Clob v) {
        try {
            return v.getSubString(1, (int) v.length());
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    protected ClobFormat getValueFormat(JsonFormat format) {
        return format.getClobFormat();
    }

    @Override
    protected Value createValue(Clob value) {
        return ValueString.get(toString(value).toString());
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getClob();
    }
}
