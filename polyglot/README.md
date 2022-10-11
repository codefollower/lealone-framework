# Lealone-Polyglot

使用 JavaScript 和 Python 语言在 Lealone 中开发微服务应用


## 编译需要

* JDK 17+ (运行只需要 JDK 1.8+)
* Maven 3.8+
* GraalVM 22.0+ (运行 Python 应用需要它，参见最后一节)


## 打包

执行以下命令打包:

`mvn package -Dmaven.test.skip=true`

生成的文件放在 `lealone-polyglot\target` 目录


## 运行 Lealone 数据库

进入 `lealone-polyglot\target` 目录，运行: `java -jar lealone-5.0.0-SNAPSHOT.jar`

```java
Lealone version: 5.0.0-SNAPSHOT
Loading config from jar:file:/home/test/lealone-polyglot/lealone-polyglot/target/lealone-5.0.0-SNAPSHOT.jar!/lealone.yaml
Base dir: .\lealone_data
Init storage engines: 5 ms
Init transaction engines: 62 ms
Init sql engines: 2 ms
Init protocol server engines: 105 ms
Init lealone database: 63 ms
Starting tcp server accepter
TcpServer started, host: 127.0.0.1, port: 9210
Web root: ./web
Sockjs path: /_lealone_sockjs_/*
HttpServer is now listening on port: 9000
HttpServer started, host: 127.0.0.1, port: 9000
Total time: 547 ms (Load config: 78 ms, Init: 241 ms, Start: 228 ms)
Exit with Ctrl+C
```

## 使用 JavaScript 开发微服务应用

/home/test/hello_service.js

```JavaScript
function hello(name) {
    return "hello " + name;
}
```

## 使用 Python 开发微服务应用

/home/test/hello_service.py

```Python
def hello(name):
    return "hello " + name;
```


## 在 Lealone 数据库中创建服务

打开一个新的命令行窗口，进入 `lealone-polyglot\target` 目录，

运行: `java -jar lealone-5.0.0-SNAPSHOT.jar -client`

执行以下 SQL 创建 js_hello_service

```java
create service js_hello_service (
  hello(name varchar) varchar
)
language 'js' implement by '/home/test/hello_service.js';
```

执行以下 SQL 创建 python_hello_service

```java
create service python_hello_service (
  hello(name varchar) varchar
)
language 'python' implement by '/home/test/hello_service.py';
```


## 通过 SQL 调用服务

execute service js_hello_service hello('zhh');

execute service python_hello_service hello('zhh');



## 通过 HTTP 调用服务

http://localhost:9000/service/js_hello_service/hello?name=zhh

http://localhost:9000/service/python_hello_service/hello?name=zhh


## 安装 GraalVM

运行 Python 应用需要事先安装 GraalVM，目前不支持 Windows

安装 GraalVM 请参考 https://www.graalvm.org/22.0/docs/getting-started/

这里假设安装后的目录是 /home/test/graalvm-ee-java17-22.0.0.2

接着配置一下 JAVA_HOME 和 PATH 环境变量

export JAVA_HOME=/home/test/graalvm-ee-java17-22.0.0.2

export PATH=$JAVA_HOME/bin:$PATH

最后还需要安装 Python 组件

gu install python

更多信息参考 https://www.graalvm.org/22.0/reference-manual/python/

