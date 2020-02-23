from lxml import etree
import requests
import csv
import time

def parse_xiaoqu(url, pa):
    head = {'Host': 'nj.lianjia.com',

            'Referer': 'https://nj.lianjia.com/chengjiao/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36'

            }

    r = requests.get(url, headers=head, timeout=5)

    html = etree.HTML(r.text)

    num = html.xpath('//div[@class="content"]//div[@class="total fl"]/span/text()')[0]

    num = int(num)

    datas = html.xpath('//li/div[@class="info"]')

    print('小区房源总数：', num, '第%d页房源数:' % pa, len(datas))

    print(url)

    if len(datas) == 0:
        return (num, [], 0)

    house_list = []

    for html1 in datas:

        title = html1.xpath('div[@class="title"]/a/text()')

        info = html1.xpath('div[@class="address"]/div[@class="houseInfo"]/text()')

        floor = html1.xpath('div[@class="flood"]/div[@class="positionInfo"]/text()')

        info[0] = info[0].replace('\xa0', '')

        date = html1.xpath('div[@class="address"]/div[@class="dealDate"]/text()')



        if date[0] == '近30天内成交':

            p_url = html1.xpath('div[@class="title"]/a/@href')

            r = requests.get(p_url[0], headers=head, timeout=5)

            html = etree.HTML(r.text)

            price = html.xpath('//div[@class="overview"]/div[@class="info fr"]/div[@class="price"]/span/i/text()')

            unitprice = html.xpath('//div[@class="overview"]/div[@class="info fr"]/div[@class="price"]/b/text()')

            date = html.xpath('//div[@class="house-title LOGVIEWDATA LOGVIEW"]/div[@class="wrapper"]/span/text()')


            if len(price) == 0:
                price.append('暂无价格')

            if len(unitprice) == 0:
                unitprice.append('暂无单价')

            date[0] = date[0].replace('链家成交', '')

            a = [title[0], info[0], floor[0], date[0], price[0], unitprice[0]]

            house_list.append(a)

            print(title[0], info[0], floor[0], date[0], price[0], unitprice[0])

        else:

            price = html1.xpath('div[@class="address"]/div[@class="totalPrice"]/span/text()')

            unitprice = html1.xpath('div[@class="flood"]/div[@class="unitPrice"]/span/text()')

            if len(price) == 0:
                price = ['暂无价格']

            if len(unitprice) == 0:
                unitprice = ['暂无单价']

            a = [title[0], info[0], floor[0], date[0], price[0], unitprice[0]]

            house_list.append(a)

            print(title[0], info[0], floor[0], date[0], price[0], unitprice[0])

    print('                *********************         ', '第%d页完成！' % pa)

    return (num, house_list, 1)


def crow_xiaoqu(id):
    url = 'https://nj.lianjia.com/chengjiao/c%d/' % int(id)

    h_list = []

    fail_list = []

    try:


        result = parse_xiaoqu(url, 1)

    except:


        time.sleep(2)

        result = parse_xiaoqu(url, 1)


    num = result[0]


    if num == 0:
        time.sleep(2)

        result = parse_xiaoqu(url, 1)

        num = result[0]

    new_list = result[1]

    pages = 1

    for data in new_list:

        if data not in h_list:
            h_list.append(data)


    if num > 30:

        if num % 30 == 0:

            pages = num // 30

        else:

            pages = num // 30 + 1

    for pa in range(2, pages + 1):

        new_url = 'https://nj.lianjia.com/chengjiao/pg' + str(pa) + 'c' + str(id)

        try:

            result = parse_xiaoqu(new_url, pa)

            status = result[2]

            if status == 1:

                new_list = result[1]


                for data in new_list:

                    if data not in h_list:
                        h_list.append(data)

            else:

                fail_list.append(pa)

        except Exception as e:

            fail_list.append(pa)

            print(e)

    print('   开始抓取第一次失败页面')

    for pa in fail_list:

        new_url = 'https://nj.lianjia.com/chengjiao/pg' + str(pa) + 'c' + str(id)

        print(new_url)

        try:

            result = parse_xiaoqu(new_url, pa)

            status = result[2]

            if status == 1:

                new_list = result[1]

                for data in new_list:

                    if data not in h_list:
                        h_list.append(data)

            else:

                pass

        except Exception as e:

            print(e)

    print('    抓取完成，开始保存数据')


    with open('lianjia_123.csv', 'a', newline='', encoding='gb18030')as f:

        write = csv.writer(f)

        for data in h_list:
            write.writerow(data)


    return (len(h_list))


if __name__ == '__main__':

    counts = 0  # 记录爬取到的房源总数

    now = time.time()

    id_list = []

    with open('xiaoqu_id.csv', 'r')as f:

        read = csv.reader(f)

        for id in read:
            id_list.append(id[0])

    m = 0


    for x in range(0, 2990):
        m += 1

        print('    开始抓取第' + str(m) + '个小区')

        time.sleep(1)

        count = crow_xiaoqu(id_list[x])

        counts = counts + count


        print('     已经抓取' + str(m) + '个小区  ' + str(counts) + '条房源信息', time.time() - now)




