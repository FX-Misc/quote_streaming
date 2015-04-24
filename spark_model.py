# read in as dataframe
# average price and total volume by second calculation
# convert time
# create features
# train model for each stock
import time
import datetime

import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row

import pandas as pd
import numpy as np

def transform_time(t):
    datestring = '2011-01-03T'
    ds = datestring + t
    return time.mktime(datetime.datetime.strptime(ds, '%Y-%m-%dT%H:%M:%S').timetuple())


def main():
    conf = SparkConf().setAppName('symbols')
    sc = SparkContext(conf=conf)

    sqlContext = SQLContext(sc)
    data = sc.textFile("file:///root/quote_streaming/data/rawdata.csv").map(lambda line: line.split(","))
    rows = data.filter(lambda x: x[0] != 'SYMBOL')
    df = rows.map(lambda p: (p[0].strip(), transform_time(p[2].strip()), float(p[3].strip()), float(p[4].strip()))) 
    
    symbols = df.map(lambda x: Row(symbol=x[0], time=x[1], price=x[2], volume=x[3]))
    schemaSymbols = sqlContext.inferSchema(symbols)
    schemaSymbols.registerTempTable("symbols")

    trades = sqlContext.sql("SELECT symbol, time, sum(price*volume)/sum(volume) as avg_price, sum(volume) as volume from symbols group by symbol, time")
    trades = trades.map(lambda x: Row(symbol=x[0], time=x[1], price=x[2], volume=x[3]))
    schemaTrades = sqlContext.inferSchema(trades)
    schemaTrades.registerTempTable("trades")

    # remove limit after test
    syms = sqlContext.sql("SELECT distinct symbol from trades LIMIT 20")
    syms = syms.collect()
    
    df_dict = {}

    for sym in syms:
        sym = sym.symbol
        sym_data = sqlContext.sql("SELECT symbol, time, price, volume FROM trades WHERE symbol = '{}'".format(sym))

        sym_data = sym_data.collect()

        # check if there are at least a threshold of trades
        if len(sym_data) < 10000:
            break
        sym_df = pd.DataFrame(sym_data, columns=['symbol', 'time', 'price', 'volume'])
        for i in range(1,11):
            sym_df['price_t-'+str(i)] = sym_df['price'].shift(i)

        for i in range(1,11):
            sym_df['volume_t-'+str(i)] = sym_df['volume'].shift(i)

        # add labels for price and volume
        sym_df['price_label'] = sym_df['price'].shift(-1)
        sym_df['volume_label'] = sym_df['volume'].shift(-1)

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
