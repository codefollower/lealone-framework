const http = require("http");
const span = require("ansispan");
require("colors");

http.createServer(function (request, response) {
    response.writeHead(200, {"Content-Type": "text/html"});
    response.end(span("Hello Graal.js!".green));
}).listen(8000, function() { console.log("Graal.js server running at http://127.0.0.1:8000/".red); });

setTimeout(function() { console.log("DONE!"); process.exit(); }, 200000);
