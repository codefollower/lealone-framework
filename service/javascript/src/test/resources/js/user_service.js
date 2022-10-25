
function crud(name) {
    // 使用 java 的类
    var User = Java.type('org.lealone.plugins.js.User');
    
    // delete 记录
    User.dao.where().name.eq(name).delete();

    // 创建 User 对象
    var user = new (User);

    // insert 记录
    user.name.set(name).phone.set(456).insert(); 

    // 查找记录
    user = User.dao.where().name.eq(name).findOne();

    return user;
}
