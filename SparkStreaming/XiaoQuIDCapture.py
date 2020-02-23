import requests
import csv
from lxml import etree
import time
import json


# 抓取小区id前100页信息
def captureXiaoquID(x):
    head = {'Host': 'nj.lianjia.com',

            'Referer': 'https://nj.lianjia.com/chengjiao/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36'

            }

    n = x * 20 + 1

    l = list(range(n, n + 20))

    for i in l:

        url = 'https://nj.lianjia.com/xiaoqu/pg' + str(i)

        try:

            r = requests.get(url, headers=head, timeout=3)

            html = etree.HTML(r.text)

            datas = html.xpath('//li[@class="clear xiaoquListItem"]/@data-id')

            title = html.xpath('//li[@class="clear xiaoquListItem"]/div[@class="info"]/div[@class="title"]/a/text()')

            print('No:' + str(x), 'page:' + str(i), len(s), len(datas), len(title))

            # 如果当前页没有返回数据，放到列表末尾等待再次抓取

            if len(datas) == 0:

                print(url)

                l.append(i)

            else:

                for data in datas:
                    s.add(data)

        # 同上，再次抓取
        except Exception as e:

            l.append(i)

            print(e)

    print('      ****No:' + str(x) + ' finish')



if __name__ == '__main__':

    global s

    s = set()
    #共30页小区
    for x in range(30):
        now = time.time()

        captureXiaoquID(x)

        print(time.time() - now)

    print('******************')

    print('抓取完成')

    # id存到本地csv

    with open('xiaoqu_id.csv', 'a', newline='', encoding='gb18030')as f:

        write = csv.writer(f)

        for data in s:
            write.writerow([data])

        f.close()














