/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.orm.format;

import java.sql.Blob;

import org.lealone.db.value.ReadonlyBlob;
import org.lealone.db.value.ValueBytes;
import org.lealone.plugins.orm.json.Json;
import org.lealone.plugins.orm.property.PBlob;

public class BlobFormat implements TypeFormat<Blob> {

    @Override
    public Object encode(Blob v) {
        return Json.BASE64_ENCODER.encodeToString(PBlob.getBytes(v));
    }

    @Override
    public Blob decode(Object v) {
        byte[] bytes;
        if (v instanceof byte[])
            bytes = (byte[]) v;
        else
            bytes = Json.BASE64_DECODER.decode(v.toString());
        return new ReadonlyBlob(ValueBytes.get(bytes));
    }
}
