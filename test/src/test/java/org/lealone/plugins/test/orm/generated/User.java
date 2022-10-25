package org.lealone.plugins.test.orm.generated;

import org.lealone.plugins.orm.Model;
import org.lealone.plugins.orm.ModelProperty;
import org.lealone.plugins.orm.ModelTable;
import org.lealone.plugins.orm.property.PArray;
import org.lealone.plugins.orm.property.PInteger;
import org.lealone.plugins.orm.property.PLong;
import org.lealone.plugins.orm.property.PString;

/**
 * Model for table 'USER'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public class User extends Model<User> {

    public static final User dao = new User(null, ROOT_DAO);

    public final PString<User> name;
    public final PString<User> notes;
    public final PInteger<User> phone;
    public final PLong<User> id;
    public final PArray<User> phones;

    public User() {
        this(null, REGULAR_MODEL);
    }

    private User(ModelTable t, short modelType) {
        super(t == null ? new ModelTable("TEST", "PUBLIC", "USER") : t, modelType);
        name = new PString<>("NAME", this);
        notes = new PString<>("NOTES", this);
        phone = new PInteger<>("PHONE", this);
        id = new PLong<>("ID", this);
        phones = new PArray<>("PHONES", this);
        super.setModelProperties(new ModelProperty[] { name, notes, phone, id, phones });
    }

    @Override
    protected User newInstance(ModelTable t, short modelType) {
        return new User(t, modelType);
    }

    public static User decode(String str) {
        return decode(str, null);
    }

    public static User decode(String str, CaseFormat format) {
        return new User().decode0(str, format);
    }
}
