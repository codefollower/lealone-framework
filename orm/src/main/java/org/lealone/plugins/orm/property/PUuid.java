/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.orm.property;

import java.util.UUID;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueUuid;
import org.lealone.plugins.orm.Model;
import org.lealone.plugins.orm.format.JsonFormat;
import org.lealone.plugins.orm.format.UuidFormat;

/**
 * UUID property.
 */
public class PUuid<M extends Model<M>> extends PBaseValueEqual<M, UUID> {

    public PUuid(String name, M model) {
        super(name, model);
    }

    @Override
    protected UuidFormat getValueFormat(JsonFormat format) {
        return format.getUuidFormat();
    }

    @Override
    protected Value createValue(UUID value) {
        return ValueUuid.get(value.getMostSignificantBits(), value.getLeastSignificantBits());
    }

    @Override
    protected void deserialize(Value v) {
        value = (UUID) ValueUuid.get(v.getBytesNoCopy()).getObject();
    }
}
