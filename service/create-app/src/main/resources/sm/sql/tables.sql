set @packageName '${packageName}.model'; -- 生成的模型类所在的包名
--set @srcDir './src/main/java';-- 生成的模型类对应的源文件所在的根目录

-- 删除表
drop table if exists user;

-- 创建表: user，会生成一个名为 User 的模型类
create table if not exists user (
  id long auto_increment primary key,
  name varchar,
  age int
) package @packageName generate code @srcDir;
