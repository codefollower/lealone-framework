set @packageName '${packageName}.service.generated'; -- 生成的服务接口所在的包名
--set @srcDir './src/main/java'; -- 生成的服务接口对应的源文件所在的根目录

-- 删除服务: hello_service
drop service if exists hello_service;

-- 创建服务: hello_service，会生成一个对应的 HelloService 接口
create service if not exists hello_service (
  say_hello(name varchar) varchar
)
package @packageName
implement by '${packageName}.service.HelloServiceImpl' -- HelloService 接口的默认实现类
generate code @srcDir;
