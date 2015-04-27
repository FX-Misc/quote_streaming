var net = require('net'),
    load = require('./load.js'),
    stream = require('./stream.js');

var server = net.createServer(function(socket) {
  socket.write('Echo server \r\n');
    
  //load();
  stream(socket);
  
});

server.listen(1337, '127.0.0.1');
