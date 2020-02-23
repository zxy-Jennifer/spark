from operator import add
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

def update_Func(new_values,last_sum):#记录上次操作状态
    """
    :type new_values: object
    """
    return sum(new_values)+(last_sum or 0)
conf = SparkConf()
conf.setAppName('TestDStream')
conf.setMaster('local[2]')
sc = SparkContext(conf = conf)
ssc = StreamingContext(sc, 20)
ssc.checkpoint('hdfs://localhost:9000/tmp/tmp')#保存检查点：设置为HDFS上的目录
lines = ssc.textFileStream('hdfs://localhost:9000/tmp')#文件目录
data1=lines.map(lambda x:x.split("', '"))
data=lines.map(lambda x:x.split("', '"))
count1=data1.map(lambda x: (x[0].replace("['", " "), 1)).updateStateByKey(update_Func)#每个小区售房总数
count1.checkpoint(20)
count1.pprint()
count2=data.map(lambda x:(x[1],1)).updateStateByKey(update_Func)#每种房型售房总数
count2.checkpoint(20)
count2.pprint()
count3=data.map(lambda x:(x[4],1)).updateStateByKey(update_Func)#每种装修类型的房子的售房总数
count3.checkpoint(20)
count3.pprint()
count4=data.map(lambda x:(x[5],1)).updateStateByKey(update_Func)#每种楼层售房总数
count4.checkpoint(20)
count4.pprint()
ssc.start()
ssc.awaitTermination()














