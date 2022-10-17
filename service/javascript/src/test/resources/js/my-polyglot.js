//js --polyglot --jvm my-polyglot.js
//node --polyglot --jvm my-polyglot.js
//polyglot --jvm my-polyglot.js

var array = new (Java.type("int[]"))(4);
array[2] = 42;
console.log(array[2]);
array[2];

var HelloPolyglot = Java.type('org.lealone.polyglot.test.HelloPolyglot');
HelloPolyglot.test();
