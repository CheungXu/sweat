import json
import scrapy
import random

class sweat_spider(scrapy.Spider):
    name = 'sweat'

    def start_requests(self):
        for i in range(1,118):
            rn = random.random()
            url = 'http://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt=kf&ft=all&rs=&gs=0&sc=zzf&st=desc&sd=2019-05-05&ed=2020-05-05&qdii=&tabSubtype=,,,,,&pi=%s&pn=50&dx=1&v=%s' % (str(i), str(rn))
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        print(response.url)

        res = response.text
        list_str = '[' + res.split(']')[0].split('[')[1] + ']'
        data_list = json.loads(list_str)

        for data in data_list:
            yield {'data': data}

