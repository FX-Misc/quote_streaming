var net = require('net'),
    load = require('./load.js'),
    stream = require('./stream.js');

var server = net.createServer(function(socket) {
  socket.write('Echo server \r\n');
  //TODO: wait to load data before streaming...
  //TODO: should the client have to ask to load data? stream data?
  //load();
  stream(socket);
  
});

server.listen(1337, '127.0.0.1');
