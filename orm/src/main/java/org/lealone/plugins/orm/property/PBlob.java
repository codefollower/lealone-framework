/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.orm.property;

import java.sql.Blob;
import java.sql.SQLException;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueBytes;
import org.lealone.plugins.orm.Model;
import org.lealone.plugins.orm.format.BlobFormat;
import org.lealone.plugins.orm.format.JsonFormat;

public class PBlob<M extends Model<M>> extends PBase<M, Blob> {

    public PBlob(String name, M model) {
        super(name, model);
    }

    public static byte[] getBytes(Blob value) {
        try {
            return value.getBytes(1, (int) value.length());
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    protected BlobFormat getValueFormat(JsonFormat format) {
        return format.getBlobFormat();
    }

    @Override
    protected Value createValue(Blob value) {
        return ValueBytes.getNoCopy(getBytes(value));
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getBlob();
    }
}
