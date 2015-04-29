from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

import json

def main():
    sym_dict = {}
    conf = SparkConf().setAppName("symbol stream")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, .1)

    lines = ssc.socketTextStream("localhost", 1337)
    
    def print_now():
        print sym_dict

    def predict(prices):
        print prices

    def add_to_dict(line):
        symbol, price, volume = line.split(',') 
        if symbol in sym_dict:
            print 'made it here'
            sym_dict[symbol][0].append(price)
            sym_dict[symbol][1].append(volume)
            if len(sym_dict[0]) > 10:
                sym_dict[0].pop(0)
                sym_dict[1].pop(0)
                predict(sym_dict[0])
        else:
            sym_dict[symbol] = [[price],[volume]]
    
    
    #test = lines.map(lambda line: json.dumps(line)) 
    test = lines.map(lambda line: line)
    test.pprint()
    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    main()
