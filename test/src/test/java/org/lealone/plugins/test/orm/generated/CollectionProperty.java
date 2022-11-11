package org.lealone.plugins.test.orm.generated;

import org.lealone.plugins.orm.Model;
import org.lealone.plugins.orm.ModelProperty;
import org.lealone.plugins.orm.ModelTable;
import org.lealone.plugins.orm.format.JsonFormat;
import org.lealone.plugins.orm.property.PList;
import org.lealone.plugins.orm.property.PMap;
import org.lealone.plugins.orm.property.PSet;

/**
 * Model for table 'COLLECTION_PROPERTY'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public class CollectionProperty extends Model<CollectionProperty> {

    public static final CollectionProperty dao = new CollectionProperty(null, ROOT_DAO);

    public final PList<CollectionProperty, Object> f1;
    public final PList<CollectionProperty, Integer> f2;
    public final PSet<CollectionProperty, Object> f3;
    public final PSet<CollectionProperty, String> f4;
    public final PMap<CollectionProperty, Object, Object> f5;
    public final PMap<CollectionProperty, Integer, String> f6;

    public CollectionProperty() {
        this(null, REGULAR_MODEL);
    }

    private CollectionProperty(ModelTable t, short modelType) {
        super(t == null ? new ModelTable("TEST", "PUBLIC", "COLLECTION_PROPERTY") : t, modelType);
        f1 = new PList<>("F1", this);
        f2 = new PList<>("F2", this);
        f3 = new PSet<>("F3", this);
        f4 = new PSet<>("F4", this);
        f5 = new PMap<>("F5", this, Object.class);
        f6 = new PMap<>("F6", this, Integer.class);
        super.setModelProperties(new ModelProperty[] { f1, f2, f3, f4, f5, f6 });
    }

    @Override
    protected CollectionProperty newInstance(ModelTable t, short modelType) {
        return new CollectionProperty(t, modelType);
    }

    public static CollectionProperty decode(String str) {
        return decode(str, null);
    }

    public static CollectionProperty decode(String str, JsonFormat format) {
        return new CollectionProperty().decode0(str, format);
    }
}
