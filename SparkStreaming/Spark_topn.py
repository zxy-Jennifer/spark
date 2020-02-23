# -*- encoding: utf8 -*-
import os
import csv
from pyspark.streaming import StreamingContext
from pyspark import SparkConf, SparkContext
os.environ['JAVA_HOME'] = "D:\zxy\JDK8"

def update_Func(new_values,last_sum):
    return  sum(new_values)+(last_sum or 0)
def func(item):
    return item[0].replace("['", "")=="墨香山庄"
conf = SparkConf()
conf.setAppName('TestDStream')
conf.setMaster('spark://Master:7077')
sc = SparkContext(conf = conf)
ssc = StreamingContext(sc, 20)
ssc.checkpoint("hdfs:///tmp/")#保存检查点：设置为HDFS上的目录
lines=ssc.textFileStream('hdfs:///tmp/')
d=lines.map(lambda x:x.split("', '"))
data=d.filter(func)
num=data.map(lambda x: (x[0].replace("['", ""), 1)).updateStateByKey(update_Func)
count=data.map(lambda x: (x[0].replace("['", ""), int(x[8]))).updateStateByKey(update_Func)
num.checkpoint(20)
count.checkpoint(20)
num.pprint()
count.pprint()
ssc.start()
ssc.awaitTermination()