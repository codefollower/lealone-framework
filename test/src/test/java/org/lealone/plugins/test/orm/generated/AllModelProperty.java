package org.lealone.plugins.test.orm.generated;

import org.lealone.plugins.orm.Model;
import org.lealone.plugins.orm.ModelProperty;
import org.lealone.plugins.orm.ModelTable;
import org.lealone.plugins.orm.format.JsonFormat;
import org.lealone.plugins.orm.property.PArray;
import org.lealone.plugins.orm.property.PBigDecimal;
import org.lealone.plugins.orm.property.PBlob;
import org.lealone.plugins.orm.property.PBoolean;
import org.lealone.plugins.orm.property.PByte;
import org.lealone.plugins.orm.property.PBytes;
import org.lealone.plugins.orm.property.PClob;
import org.lealone.plugins.orm.property.PDate;
import org.lealone.plugins.orm.property.PDouble;
import org.lealone.plugins.orm.property.PFloat;
import org.lealone.plugins.orm.property.PInteger;
import org.lealone.plugins.orm.property.PLong;
import org.lealone.plugins.orm.property.PObject;
import org.lealone.plugins.orm.property.PShort;
import org.lealone.plugins.orm.property.PString;
import org.lealone.plugins.orm.property.PTime;
import org.lealone.plugins.orm.property.PTimestamp;
import org.lealone.plugins.orm.property.PUuid;

/**
 * Model for table 'ALL_MODEL_PROPERTY'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public class AllModelProperty extends Model<AllModelProperty> {

    public static final AllModelProperty dao = new AllModelProperty(null, ROOT_DAO);

    public final PInteger<AllModelProperty> f1;
    public final PBoolean<AllModelProperty> f2;
    public final PByte<AllModelProperty> f3;
    public final PShort<AllModelProperty> f4;
    public final PLong<AllModelProperty> f5;
    public final PLong<AllModelProperty> f6;
    public final PBigDecimal<AllModelProperty> f7;
    public final PDouble<AllModelProperty> f8;
    public final PFloat<AllModelProperty> f9;
    public final PTime<AllModelProperty> f10;
    public final PDate<AllModelProperty> f11;
    public final PTimestamp<AllModelProperty> f12;
    public final PBytes<AllModelProperty> f13;
    public final PObject<AllModelProperty> f14;
    public final PString<AllModelProperty> f15;
    public final PString<AllModelProperty> f16;
    public final PString<AllModelProperty> f17;
    public final PBlob<AllModelProperty> f18;
    public final PClob<AllModelProperty> f19;
    public final PUuid<AllModelProperty> f20;
    public final PArray<AllModelProperty> f21;

    public AllModelProperty() {
        this(null, REGULAR_MODEL);
    }

    private AllModelProperty(ModelTable t, short modelType) {
        super(t == null ? new ModelTable("TEST", "PUBLIC", "ALL_MODEL_PROPERTY") : t, modelType);
        f1 = new PInteger<>("F1", this);
        f2 = new PBoolean<>("F2", this);
        f3 = new PByte<>("F3", this);
        f4 = new PShort<>("F4", this);
        f5 = new PLong<>("F5", this);
        f6 = new PLong<>("F6", this);
        f7 = new PBigDecimal<>("F7", this);
        f8 = new PDouble<>("F8", this);
        f9 = new PFloat<>("F9", this);
        f10 = new PTime<>("F10", this);
        f11 = new PDate<>("F11", this);
        f12 = new PTimestamp<>("F12", this);
        f13 = new PBytes<>("F13", this);
        f14 = new PObject<>("F14", this);
        f15 = new PString<>("F15", this);
        f16 = new PString<>("F16", this);
        f17 = new PString<>("F17", this);
        f18 = new PBlob<>("F18", this);
        f19 = new PClob<>("F19", this);
        f20 = new PUuid<>("F20", this);
        f21 = new PArray<>("F21", this);
        super.setModelProperties(new ModelProperty[] { f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21 });
    }

    @Override
    protected AllModelProperty newInstance(ModelTable t, short modelType) {
        return new AllModelProperty(t, modelType);
    }

    public static AllModelProperty decode(String str) {
        return decode(str, null);
    }

    public static AllModelProperty decode(String str, JsonFormat format) {
        return new AllModelProperty().decode0(str, format);
    }
}
