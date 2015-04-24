from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext("local[2]", "symbols")
ssc = StreamingContext(sc, 1)

lines = ssc.socketTextStream("localhost", 1337)

lines.pprint()
ssc.start()
ssc.awaitTermination()
