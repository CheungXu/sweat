import json
import scrapy

class sweat_spider(scrapy.Spider):
    name = 'sweat'

    def start_requests(self):
        urls = ['http://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt=kf&ft=all&rs=&gs=0&sc=zzf&st=desc&sd=2019-05-05&ed=2020-05-05&qdii=&tabSubtype=,,,,,&pi=1&pn=50&dx=1&v=0.8578375131593362']
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        print(response.status)
        print(response.headers)
        print(len(response.text))

        res = response.text
        list_str = '[' + res.split(']')[0].split('[')[1] + ']'
        data_list = json.loads(list_str)

        for d in data_list:
            print(d)

        