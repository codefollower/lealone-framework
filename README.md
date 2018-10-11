# Lealone-Plugins
与 Lealone 集成的各类插件(例如网络框架、不同数据库的协议与SQL语法、存储引擎)

## 网络层插件

### [lealone-mina](https://github.com/lealone/Lealone-Plugins/tree/master/lealone-mina)
使用 [Apache MINA](http://mina.apache.org/) 网络应用框架来实现 Lealone 客户端与服务器端以及 Lealone 集群节点之间的网络通信


### [lealone-netty](https://github.com/lealone/Lealone-Plugins/tree/master/lealone-netty)
使用 [Netty](http://netty.io/) 网络应用框架来实现 Lealone 客户端与服务器端以及 Lealone 集群节点之间的网络通信


### [lealone-vertx](https://github.com/lealone/Lealone-Plugins/tree/master/lealone-vertx)
使用 [Vert.x](https://vertx.io/) 来实现 Lealone 客户端与服务器端以及 Lealone 集群节点之间的网络通信


## 存储层插件

### [lealone-wiredtiger](https://github.com/lealone/Lealone-Plugins/tree/master/lealone-wiredtiger)
使用 [WiredTiger](http://www.wiredtiger.com/) 作为额外的 Lealone 存储引擎


### [lealone-mvstore](https://github.com/lealone/Lealone-Plugins/tree/master/lealone-mvstore)
使用 [H2](http://www.h2database.com/html/main.html) 数据库的 [MVStore](http://www.h2database.com/html/mvstore.html) 作为额外的 Lealone 存储引擎


### [lealone-rocksdb](https://github.com/lealone/Lealone-Plugins/tree/master/lealone-rocksdb)
使用 [RocksDB](https://github.com/facebook/rocksdb) 作为额外的 Lealone 存储引擎


## 数据库协议与SQL语法插件


### [lealone-postgresql](https://github.com/lealone/Lealone-Plugins/tree/master/lealone-postgresql)
支持现有的 [PostgreSQL](https://www.postgresql.org/) 客户端访问 Lealone 数据库


### [lealone-mysql](https://github.com/lealone/Lealone-Plugins/tree/master/lealone-mysql)
支持现有的 [MySQL](https://www.mysql.com/) 客户端用MySQL的协议和SQL语法访问 Lealone 数据库
