/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.orm.property;

import java.sql.Clob;
import java.sql.SQLException;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.value.ReadonlyClob;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueString;
import org.lealone.plugins.orm.Model;

public class PClob<M extends Model<M>> extends PBase<M, Clob> {

    public PClob(String name, M model) {
        super(name, model);
    }

    @Override
    protected Value createValue(Clob value) {
        return ValueString.get(encodeValue().toString());
    }

    @Override
    protected Object encodeValue() {
        try {
            return value.getSubString(1, (int) value.length());
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getClob();
    }

    @Override
    protected void deserialize(Object v) {
        value = new ReadonlyClob(v.toString());
    }
}
