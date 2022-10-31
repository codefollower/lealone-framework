package org.lealone.plugins.test.orm.generated;

import org.lealone.plugins.orm.Model;
import org.lealone.plugins.orm.ModelProperty;
import org.lealone.plugins.orm.ModelTable;
import org.lealone.plugins.orm.format.JsonFormat;
import org.lealone.plugins.orm.property.PBoolean;
import org.lealone.plugins.orm.property.PInteger;
import org.lealone.plugins.orm.property.PLong;

/**
 * Model for table 'JSON_TEST_TABLE'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public class JsonTestTable extends Model<JsonTestTable> {

    public static final JsonTestTable dao = new JsonTestTable(null, ROOT_DAO);

    public final PInteger<JsonTestTable> propertyName1;
    public final PLong<JsonTestTable> propertyName2;
    public final PBoolean<JsonTestTable> b;

    public JsonTestTable() {
        this(null, REGULAR_MODEL);
    }

    private JsonTestTable(ModelTable t, short modelType) {
        super(t == null ? new ModelTable("TEST", "PUBLIC", "JSON_TEST_TABLE") : t, modelType);
        propertyName1 = new PInteger<>("PROPERTY_NAME1", this);
        propertyName2 = new PLong<>("PROPERTY_NAME2", this);
        b = new PBoolean<>("B", this);
        super.setJsonFormat("lower_underscore_format");
        super.setModelProperties(new ModelProperty[] { propertyName1, propertyName2, b });
    }

    @Override
    protected JsonTestTable newInstance(ModelTable t, short modelType) {
        return new JsonTestTable(t, modelType);
    }

    public static JsonTestTable decode(String str) {
        return decode(str, null);
    }

    public static JsonTestTable decode(String str, JsonFormat format) {
        return new JsonTestTable().decode0(str, format);
    }
}
