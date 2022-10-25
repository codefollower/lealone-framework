package org.lealone.plugins.test.orm.generated;

import org.lealone.plugins.orm.Model;
import org.lealone.plugins.orm.ModelProperty;
import org.lealone.plugins.orm.ModelTable;
import org.lealone.plugins.orm.property.PDouble;
import org.lealone.plugins.orm.property.PLong;
import org.lealone.plugins.orm.property.PString;

/**
 * Model for table 'PRODUCT'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public class Product extends Model<Product> {

    public static final Product dao = new Product(null, ROOT_DAO);

    public final PLong<Product> productId;
    public final PString<Product> productName;
    public final PString<Product> category;
    public final PDouble<Product> unitPrice;

    public Product() {
        this(null, REGULAR_MODEL);
    }

    private Product(ModelTable t, short modelType) {
        super(t == null ? new ModelTable("TEST", "PUBLIC", "PRODUCT") : t, modelType);
        productId = new PLong<>("PRODUCT_ID", this);
        productName = new PString<>("PRODUCT_NAME", this);
        category = new PString<>("CATEGORY", this);
        unitPrice = new PDouble<>("UNIT_PRICE", this);
        super.setModelProperties(new ModelProperty[] { productId, productName, category, unitPrice });
    }

    @Override
    protected Product newInstance(ModelTable t, short modelType) {
        return new Product(t, modelType);
    }

    public static Product decode(String str) {
        return decode(str, null);
    }

    public static Product decode(String str, CaseFormat format) {
        return new Product().decode0(str, format);
    }
}
