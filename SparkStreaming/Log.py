# encoding: utf-8
import sys
import io
import sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf-8')
import csv
import  random
file =  open('C://Users//Jiaot//Desktop//LianjiaData//lianjia_123.csv','r') #dataSource dir .csv
lines = file.readlines()
row = [];
for line in lines:
    row.append(line.split(','))
for i in range(100):
    print(str(row[random.randint(0,40000)]))

