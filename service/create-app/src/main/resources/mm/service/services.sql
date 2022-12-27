set @packageName '${packageName}.service.generated'; -- 生成的服务接口所在的包名
--set @srcDir './src/main/java'; -- 生成的服务接口对应的源文件所在的根目录

-- 删除服务: ${dbName}_service
drop service if exists ${dbName}_service;

-- 创建服务: ${dbName}_service，会生成一个对应的 ${appClassName}Service 接口
create service if not exists ${dbName}_service (
  hello(name varchar) varchar,
  say_bye() varchar
)
package @packageName
implement by '${packageName}.service.${appClassName}ServiceImpl' -- ${appClassName}Service 接口的默认实现类
generate code @srcDir;
