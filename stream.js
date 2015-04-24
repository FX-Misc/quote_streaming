var stream = function(socket) {
  console.log("Subscribing to topic");

  var client = redis.createClient()
  client.on('error', function(error) {
    console.log(error.message)
  });
  if (client) console.log("connected to redis");

  function send(obj, start) {
      socket.write(JSON.stringify(obj)+'\r\n');
  }

  var startTime = 1294075819;
  //Set end time to end of day?
  var endTime = startTime + 200;

  function fetchData() {
    if (startTime < endTime) {
      client.hgetall('test:'+startTime, function(err, obj) {
        send(obj, startTime);
      });
      startTime++;
    }
    else {
      clearInterval(emitData);
      console.log("no more data");
    }
  }
  var emitData = setInterval(fetchData, 1000);
}

exports = module.exports = stream
