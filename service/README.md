# Lealone 微服务框架

## 在 pom.xml 中增加依赖

```xml
    <dependencies>
        <!--引入 Lealone 微服务框架 -->
        <dependency>
            <groupId>org.lealone.plugins</groupId>
            <artifactId>lealone-service</artifactId>
            <version>5.2.0</version>
        </dependency>

        <!-- 使用 Vertx 作为 HTTP Server -->
        <dependency>
            <groupId>org.lealone.plugins</groupId>
            <artifactId>lealone-vertx</artifactId>
            <version>5.2.0</version>
        </dependency>

        <!-- 也可以使用 Tomcat 作为 HTTP Server -->
        <dependency>
            <groupId>org.lealone.plugins</groupId>
            <artifactId>lealone-tomcat</artifactId>
            <version>5.2.0</version>
        </dependency>

        <!-- 使用 JavaScript 开发微服务应用-->
        <dependency>
            <groupId>org.lealone.plugins</groupId>
            <artifactId>lealone-javascript</artifactId>
            <version>5.2.0</version>
        </dependency>

        <!-- 使用 Python 开发微服务应用-->
        <dependency>
            <groupId>org.lealone.plugins</groupId>
            <artifactId>lealone-python</artifactId>
            <version>5.2.0</version>
        </dependency>
    </dependencies>

    <!-- 如果使用 SNAPSHOT 版本，需要加上这个 -->
    <repositories>
        <repository>
            <id>ossrh</id>
            <url>https://oss.sonatype.org/content/repositories/snapshots</url>
        </repository>
    </repositories>
```

# Lealone 微服务应用脚手架

用于创建 Lealone 微服务应用的脚手架，请阅读[文档](https://github.com/lealone/Lealone-Plugins/blob/master/service/create-app/README.md)


# Lealone Polyglot

使用 JavaScript 和 Python 语言在 Lealone 中开发微服务应用


## 编译需要

* JDK 17+ (运行只需要 JDK 1.8+)
* Maven 3.8+
* GraalVM 22.0+ (运行 Python 应用需要它，参见最后一节)


## 打包

执行以下命令打包:

`mvn package -Dmaven.test.skip=true`

生成的文件放在 `lealone-plugins\target` 目录


## 运行 Lealone 数据库

进入 `lealone-plugins\target` 目录，运行: `java -jar lealone-polyglot-5.2.0.jar`

```java
Lealone version: 5.2.0
Loading config from jar:file:/E:/lealone/lealone-plugins/target/lealone-polyglot-5.2.0.jar!/lealone.yaml
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

E:/test/hello_service.js

```JavaScript
function hello(name) {
    return "hello " + name;
}
```

## 使用 Python 开发微服务应用

E:/test/hello_service.py

```Python
def hello(name):
    return "hello " + name;
```


## 在 Lealone 数据库中创建服务

打开一个新的命令行窗口，进入 `lealone-plugins\target` 目录，

运行: `java -jar lealone-polyglot-5.2.0.jar -client`

执行以下 SQL 创建 js_hello_service

```java
create service js_hello_service (
  hello(name varchar) varchar
)
language 'js' implement by 'E:/test/hello_service.js';
```

执行以下 SQL 创建 python_hello_service

```java
create service python_hello_service (
  hello(name varchar) varchar
)
language 'python' implement by 'E:/test/hello_service.py';
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

