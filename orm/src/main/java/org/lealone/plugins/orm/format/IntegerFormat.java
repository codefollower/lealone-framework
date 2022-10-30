/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.orm.format;

public class IntegerFormat implements TypeFormat<Integer> {

    @Override
    public Object encode(Integer v) {
        return v;
    }

    @Override
    public Integer decode(Object v) {
        return ((Number) v).intValue();
    }
}
