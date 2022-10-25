/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.test.orm;

import org.junit.Before;
import org.junit.Test;
import org.lealone.plugins.orm.Model.CaseFormat;
import org.lealone.plugins.orm.json.JsonObject;
import org.lealone.plugins.test.orm.generated.JsonTestTable;
import org.lealone.plugins.test.orm.generated.User;

public class OrmJsonTest extends OrmTestBase {

    @Before
    @Override
    public void setUpBefore() {
        setEmbedded(true);
        setInMemory(true);
        SqlScript.createUserTable(this);
        SqlScript.createJsonTestTable(this);
    }

    @Test
    public void run() {
        testModelType();
        testCaseFormat();
    }

    void testModelType() {
        // dao对象序列化后包含modelType字段，并且是ROOT_DAO
        JsonObject json = new JsonObject(User.dao.encode());
        assertTrue(json.getInteger("modelType") == User.ROOT_DAO);

        // 反序列化
        String str = json.encode();
        User u = User.decode(str);
        assertTrue(u.isDao());

        // 普通User对象序列化后也包含modelType字段，但为REGULAR_MODEL
        json = new JsonObject(new User().encode());
        assertTrue(json.getInteger("modelType") == User.REGULAR_MODEL);
    }

    void testCaseFormat() {
        User user = new User().id.set(1006).name.set("rob6").phones.set(new Object[] { 1, 2, 3 });
        String json = user.encode();
        JsonObject jsonObject = new JsonObject(json);
        assertEquals(jsonObject.getString(User.dao.name.getName()), user.name.get());
        User u = User.decode(json);
        assertEquals(u.name.get(), user.name.get());

        json = user.encode(CaseFormat.CAMEL);
        jsonObject = new JsonObject(json);
        assertEquals(jsonObject.getString(User.dao.name.getName(CaseFormat.CAMEL)), user.name.get());

        JsonTestTable t1 = new JsonTestTable().propertyName1.set(1).propertyName2.set(3);
        json = t1.encode();
        jsonObject = new JsonObject(json);
        assertEquals(jsonObject.getInteger(JsonTestTable.dao.propertyName1.getName(CaseFormat.CAMEL)),
                t1.propertyName1.get());
        JsonTestTable t2 = JsonTestTable.decode(json);
        assertEquals(t1.propertyName2.get(), t2.propertyName2.get());

        json = t1.encode(CaseFormat.LOWER_UNDERSCORE);
        t2 = JsonTestTable.decode(json, CaseFormat.LOWER_UNDERSCORE);
        assertEquals(t1.propertyName2.get(), t2.propertyName2.get());
    }
}
