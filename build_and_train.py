import time
import datetime
import config

import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row

import pandas as pd
import numpy as np

from pyspark.mllib.classification import LogisticRegressionWithSGD, SVMWithSGD
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD
from pyspark.storagelevel import StorageLevel
from numpy import array

from sklearn.cross_validation import train_test_split
from sklearn.metrics import classification_report,f1_score,accuracy_score
from sklearn.linear_model import SGDClassifier
from math import ceil

#Iterates through dfTrain, building the Y vector
def buildY(dfTrain):

    #labelName = 'price_dir_label'
    labelName = 'volume_dir_label'
    #labelName = 'tnext_dir'
    #labelName = 'tnext'

    Y = dfTrain[labelName].values

    return Y

#Iterates through dfTrain, building the X matrix
def buildX(dfTrain):
    begintime = time.time()

    #features = ['price', 'price_t-1', 'price_t-2', 'price_t-3', 'volume', 'volume_t-1', 'volume_t-2', 'volume_t-3']
    features = ['price', 'price_t-1', 'price_t-2', 'volume10k', 'volume10k_t-1']
    #features = ['AVG_PRICE', 't1', 't2', 't3']
    print "features: " + ', '.join(features)

    X = dfTrain.as_matrix(features)

    endtime = time.time()
    totaltime = endtime-begintime
    print "building x time: " + str(totaltime)

    return X

#Splits dataset into train and test (so that we can evaluate the model)
def split(X,Y):
    Xtrain,Xtest,Ytrain,Ytest = train_test_split(X, Y, test_size=0.20)
    return Xtrain,Xtest,Ytrain,Ytest

#Trains and return the model
def buildModel(trainrdd):

    model = LogisticRegressionWithSGD.train(trainrdd)
    #model = LinearRegressionWithSGD.train(trainrdd)
    return model

#Returns prediction of whether post should be closed or open for a single post
def getPredictions(singleXtest):

    return model.predict(singleXtest)

#Iterates through test vectors, predicting output category for each one
#Then compares to true labels and prints evaluation metrics
def handlePrediction(model,Xtest,Ytest):
    predicted = map(getPredictions,Xtest)

    print "multiclass"
    print classification_report(Ytest, predicted)

    print "overall accuracy: " + str(accuracy_score(Ytest, predicted, normalize=True))

#Builds a single LabeledPoint out of tuple of an x row and corresponding y value
#Spark requires a certain format--hence transposing and sorting
def parsePoint(trainingTuple):
    return LabeledPoint(trainingTuple[1], trainingTuple[0])


#Builds Spark rdd out of X and Y
def buildLabeledPoints(X,Y):
    trainingTuples = zip(X,Y)

    labeledPoints=map(parsePoint,trainingTuples)

    rdd = sc.parallelize(labeledPoints)
    return rdd


def train(dfTrain):
    Y = buildY(dfTrain)

    X = buildX(dfTrain)

    Xtrain,Xtest,Ytrain,Ytest = split(X,Y)
    print "data split............."

    trainrdd = buildLabeledPoints(Xtrain,Ytrain)
    print "rdd built..........."

    print trainrdd.count()

    model = buildModel(trainrdd)
    print "model built..........."

    handlePrediction(model,Xtest,Ytest)
    print "prediction handled..........."


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
    conf = SparkConf().setAppName('buildtrain')
    sc = SparkContext(conf=conf)

    sqlContext = SQLContext(sc)
    data = sc.textFile("hdfs://spark1:9000/user/convert_out/ct_20110218.csv", 200).map(lambda line: line.split(","))
    rows = data.filter(lambda x: x[0] != 'SYMBOL')
    df = rows.map(lambda p: (p[0].strip(), transform_time(p[1].strip(), p[2].strip()), float(p[3].strip()), float(p[4].strip())))

    symbols = df.map(lambda x: Row(symbol=x[0], time=x[1], price=x[2], volume=x[3]))
    schemaSymbols = sqlContext.inferSchema(symbols)
    schemaSymbols.registerTempTable("symbols")

    trades = sqlContext.sql("""SELECT symbol, time, sum(price*volume)/sum(volume) as price, sum(volume) as volume from
            symbols group by symbol, time""")
    trades = trades.map(lambda x: Row(symbol=x[0], time=x[1], price=x[2], volume=x[3]))
    schemaTrades = sqlContext.inferSchema(trades)
    schemaTrades.registerTempTable("trades")

    # remove limit after test
    syms = sqlContext.sql("SELECT distinct symbol from trades where symbol = 'C'")
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

        train(sym_df)

    # print for testing
    print len(df_dict)
    print df_dict.keys()
    print type(df_dict[sym])
    sc.stop()

if __name__ == '__main__':
    main()
