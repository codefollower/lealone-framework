
drop table if  exists user;

create table if not exists user (
  name char(10) primary key,
  notes varchar,
  phone int,
  id long,
  phones ARRAY
)
package 'org.lealone.plugins.js' generate code './src/test/java';


create service if not exists hello_service (
  hello(name varchar) varchar
)
language 'js' implement by './src/test/resources/js/hello_service.js';


create service if not exists user_service (
  crud(name varchar) varchar
)
language 'js' implement by './src/test/resources/js/user_service.js';


--execute service hello_service hello('test');

--execute service user_service crud('test');
