parse = require('csv-parse'),
redis = require('redis'),
moment = require('moment'),
fs = require('fs');

var stockQuote = function() {

  var client = redis.createClient()
  client.on('error', function(error) {
    console.log(error.message)
  });

  function loadQuote(callback) {
    var i = 0
    var parser = parse({delimiter: ',', columns: true})
    var lastTime, price, volume, symbol, ts;
    var count = 0;
    var init = false;
    parser.on('readable', function(data) {
      while(data=parser.read()) {
        var datestring = '2011-01-03T';
        datestring += data['TIME'];
        ts = moment(datestring).unix()

        if(ts != lastTime || data['SYMBOL'] != symbol) {
          if (!init) {
            //if vars not initialized, set them
            lastTime = ts;
            price = parseFloat(data['PRICE']);
            volume = parseFloat(data['SIZE']);
            symbol = data['SYMBOL'];
            count += 1;
            init = true;
          }
          else {
            //else, commit prior data, set new vars, reset count
            var setVal = price+':'+volume;
            client.hset('test:'+ts,
                         symbol, setVal);
            console.log("loaded symbol " + symbol + " at: " + setVal);
            lastTime = ts;
            price = parseFloat(data['PRICE']);
            volume = parseFloat(data['SIZE']);
            symbol = data['SYMBOL'];
            count = 0;
          }
        }
        else {
          //if ts and symbol are the same, update count price, volume
          count += 1
          price = (price * (count - 1) + parseFloat(data['PRICE'])) / count;
          volume += parseFloat(data['SIZE']);

        }
      }
    });
    rs = fs.createReadStream(__dirname + '/data/rawdata.csv')
    rs.pipe(parser)
  }
  loadQuote();
}

exports = module.exports = stockQuote
