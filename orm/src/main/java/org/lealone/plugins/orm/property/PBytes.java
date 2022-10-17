/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.orm.property;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueBytes;
import org.lealone.plugins.orm.Model;
import org.lealone.plugins.orm.json.Json;

/**
 * byte[] property. 
 */
public class PBytes<M extends Model<M>> extends PBase<M, byte[]> {

    public PBytes(String name, M model) {
        super(name, model);
    }

    @Override
    protected Value createValue(byte[] value) {
        return ValueBytes.getNoCopy(value);
    }

    @Override
    protected Object encodeValue() {
        return Json.BASE64_ENCODER.encodeToString(value);
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getBytes();
    }

    @Override
    protected void deserialize(Object v) {
        value = Json.BASE64_DECODER.decode(v.toString());
    }
}
