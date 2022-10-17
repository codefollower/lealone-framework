
create service if not exists hello_service (
  hello(name varchar) varchar
)
language 'python' implement by './src/test/resources/python/hello_service.py'
;

--execute service hello_service hello('test');
