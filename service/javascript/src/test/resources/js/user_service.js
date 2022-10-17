
function crud(name) {
    // 使用 java 的类
    var User = Java.type('org.lealone.polyglot.test.User');

    // 创建 User 对象
    var user = new (User);

    // insert 记录
    user.name.set(name).phone.set(456).insert(); 

    // 查找记录
    var user = User.dao.where().name.eq(name).findOne();

    return user;
}
