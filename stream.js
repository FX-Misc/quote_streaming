var stream = function(socket) {
  console.log("Subscribing to topic");

  var client = redis.createClient()
  client.on('error', function(error) {
    console.log(error.message)
  });

  function send(obj, start) {
      socket.write(JSON.stringify(obj), start)
  }

  var i = 0
  var startTime = 1294075819;
  //Set end time to end of day?
  var endTime = startTime + 20;

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
  var emitData = setInterval(fetchData, 100);
}

exports = module.exports = stream
