$(function() {
    var stream = new EventSource("/test");
    console.log(stream)
    stream.addEventListener("message",function(e) {
	      var data = e.data;
	      try {
	         data = JSON.parse(data);
	         if(data.price && data.volume) {
		           $('#timestamp').html(""+(new Date(data.TIMESTAMP)));
		           $('#symbol').html(data.symbol);
		           $('#price').html(data.price);
               $('#volume').html(data.volume);
	         }
	      } catch(e) { console.log(e); }
    });
});
