import time
import datetime

import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row

import pandas as pd
import numpy as np

def transform_time(dys, t):
    dys = int(float(dys))
    epoch = datetime.datetime(1960,1,1)
    days = epoch + datetime.timedelta(days=int(dys))
    datestring = '{}-{}-{}T'.format(days.year, days.month, days.day)
    ds = datestring + t
    i = 1
    try: 
        dte = time.mktime(datetime.datetime.strptime(ds, '%Y-%m-%dT%H:%M:%S').timetuple())
    except ValueError:
        dte = float(i)
        i += 1
    return dte


def main():
    conf = SparkConf().setAppName('symbols').set("spark.storage.blockManagerSlaveTimeoutMs", 60000)
    sc = SparkContext(conf=conf)

    sqlContext = SQLContext(sc)
    data = sc.textFile("hdfs://spark1:9000/user/convert_out/ct_20110218.csv", 200).map(lambda line: line.split(",")).cache()
    rows = data.filter(lambda x: x[0] != 'SYMBOL')
    df = rows.map(lambda p: (p[0].strip(), transform_time(p[1].strip(), p[2].strip()), float(p[3].strip()), float(p[4].strip()))) 
    #df = df.filter(lambda x: x[1] != 0)    

    symbols = df.map(lambda x: Row(symbol=x[0], time=x[1], price=x[2], volume=x[3]))
    schemaSymbols = sqlContext.inferSchema(symbols)
    schemaSymbols.registerTempTable("symbols")
    
    trades = sqlContext.sql("""SELECT symbol, time, sum(price*volume)/sum(volume) as price, sum(volume) as volume from
            symbols group by symbol, time""")
    trades = trades.map(lambda x: Row(symbol=x[0], time=x[1], price=x[2], volume=x[3]))
    schemaTrades = sqlContext.inferSchema(trades)
    schemaTrades.registerTempTable("trades")

    # remove limit after test
    syms = sqlContext.sql("SELECT distinct symbol from trades")
    syms = syms.collect()
    
    df_dict = {}
    print type(syms)
    for sym in syms:
        sym = sym.symbol.strip()
        print sym
        sym_data = sqlContext.sql("SELECT symbol, time, price, volume FROM trades WHERE symbol = '{}' ORDER BY symbol, time".format(sym))
        
        sym_data = sym_data.collect()
        print len(sym_data)
        sym_df = pd.DataFrame(sym_data, columns=['symbol', 'time', 'price', 'volume'])
        for i in range(1,11):
            sym_df['price_t-'+str(i)] = sym_df['price'].shift(i)

        for i in range(1,11):
            sym_df['volume_t-'+str(i)] = sym_df['volume'].shift(i)

        # add labels for price and volume
        sym_df['price_label'] = sym_df['price'].shift(-1)
        sym_df['volume_label'] = sym_df['volume'].shift(-1)
        
        sym_df['price_label'] = np.where(sym_df.price_label > sym_df.price, 1, 0)
        sym_df['volume_label'] = np.where(sym_df.volume_label > sym_df.volume, 1, 0)


        sym_df = sym_df.dropna()
        df_dict[sym] = sym_df
        print sym_df

    # print for testing
    print len(df_dict)
    print df_dict.keys()
    print type(df_dict[sym])
    sc.stop()

if __name__ == '__main__':
    main()
