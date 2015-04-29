var stream = function(socket) {
  console.log("Beginning Stream");

  var client = redis.createClient()
  client.on('error', function(error) {
    console.log(error.message)
  });
  if (client) console.log("connected to redis");

  var startTime = 1294075819;
  //Set end time to end of day?
  var endTime = startTime + 2000;

  function fetchData() {
    if (startTime < endTime) {
      client.hgetall('test:'+startTime, function(err, obj) {
        for (var k in obj) {
         
          var data = obj[k].split(':');
          var price = data[0];
          var volume = data[1];
          socket.write(JSON.stringify({"symbol": k, "price": price, "volume": volume})+'\r\n');
        }
      });
      startTime++;
    }
    else {
      clearInterval(emitData);
      console.log("no more data");
    }
  }
  var emitData = setInterval(fetchData, 10);
}

exports = module.exports = stream
