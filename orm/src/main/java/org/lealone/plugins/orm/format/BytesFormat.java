/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.orm.format;

import org.lealone.plugins.orm.json.Json;

public class BytesFormat implements TypeFormat<byte[]> {

    @Override
    public Object encode(byte[] v) {
        return Json.BASE64_ENCODER.encodeToString(v);
    }

    @Override
    public byte[] decode(Object v) {
        return Json.BASE64_DECODER.decode(v.toString());
    }
}
