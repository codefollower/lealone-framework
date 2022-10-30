/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.orm.format;

import org.lealone.plugins.orm.json.Json;

public class ObjectFormat implements TypeFormat<Object> {

    @Override
    public Object encode(Object v) {
        return Json.encode(v);
    }

    @Override
    public Object decode(Object v) {
        return Json.decode(v.toString());
    }
}
