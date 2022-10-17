# Lealone-Create-App

用于创建 Lealone 应用的脚手架



### 构建

mvn package -Dmaven.test.skip=true



### 运行

java -jar ./target/create-lealone-app-5.0.0.jar [选项]

支持以下选项：

        -help
                打印帮助信息

        -appBaseDir <dir>
                应用根目录 (默认是当前目录)

        -appName <name>
                应用名称 (如果不指定，默认取 -artifactId 的值，-appName 和 -artifactId 必须至少设置一个)

        -groupId <id>
                pom.xml 的 groupId (必须设置)

        -artifactId <id>
                pom.xml 的 artifactId  (如果不指定，默认取 -appName 的值，-appName 和 -artifactId 必须至少设置一个)

        -version <版本号>
                pom.xml 的项目初始版本号 (默认是1.0.0)

        -packageName <包名>
                项目代码的包名 (如果不指定，默认取 -groupId 的值)

        -encoding <编码>
                指定生成的文件采用的编码 (默认是 UTF-8)


### 例如创建一个 hello 应用

java -jar ./target/create-lealone-app-5.0.0.jar -groupId org.lealone.examples.hello -artifactId hello


### 构建 hello 应用

cd hello

build -p


### 运行 hello 应用

cd target\hello-1.0.0\bin

lealone


### 打开页面

http://localhost:9000/

