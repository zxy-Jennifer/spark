Set timevar = %1
echo %timevar%
hdfs dfs -put %timevar% /tmp
timeout /T 3 /NOBREAK
exit
