/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.orm.format;

public class BooleanFormat implements TypeFormat<Boolean> {

    @Override
    public Object encode(Boolean value) {
        return value ? 1 : 0;
    }

    @Override
    public Boolean decode(Object v) {
        return ((Number) v).byteValue() != 0;
    }
}
