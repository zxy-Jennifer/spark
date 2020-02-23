#!/bin/bash
#HDFS命令
#HDFS="F:/hadoop-2.8.3/bin/hadoop fs"
#Streaming监听的文件目录
streaming_dir="/tmp"
##清空旧数据
#$HDFS -rm "${streaming_dir}"'/tmp/*'>/dev/null 2>&1
#$HDFS -rm "${streaming_dir}"'/*'>/dev/null 2>&1

#生成日志
while [ 1 ];do
    python Log.py > test.log
    #加时间戳防止重名
    tmplog="access.`date +'%s'`.log"
    echo "`date +"%F %T"` put $tmplog to HDFS succeed"
    hdfs dfs -put test.log ${streaming_dir}/tmp/$tmplog
    hdfs dfs -mv ${streaming_dir}/tmp/$tmplog ${streaming_dir}/
    sleep 1
done
