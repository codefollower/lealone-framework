/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.orm.format;

public class DoubleFormat implements TypeFormat<Double> {

    @Override
    public Object encode(Double v) {
        return v;
    }

    @Override
    public Double decode(Object v) {
        return ((Number) v).doubleValue();
    }
}
