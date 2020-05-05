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
            data_list = data.split(',')
            
            fund_id = data_list[0]
            fund_name = data_list[1]
            date = data_list[3]
            per_fund_vale = data_list[4]
            total_fund_value = data_list[5]
            fund_value_rate_day = data_list[6]
            fund_value_rate_week = data_list[7]
            fund_value_rate_month = data_list[8]
            fund_value_rate_3month = data_list[9]
            fund_value_rate_6month = data_list[10]
            fund_value_rate_year = data_list[11]
            fund_value_rate_2year = data_list[12]
            fund_value_rate_3year = data_list[13]
            fund_value_this_year = data_list[14]
            fund_value_from_establish = data_list[15]
            yield { 'data_type': 'fund list',
                    'fund_id': fund_id,
                    'fund_name': fund_name,
                    'date': date,
                    'per_fund_vale': per_fund_vale,
                    'total_fund_value': total_fund_value,
                    'fund_value_rate_day': fund_value_rate_day,
                    'fund_value_rate_week': fund_value_rate_week,
                    'fund_value_rate_month': fund_value_rate_month,
                    'fund_value_rate_3month': fund_value_rate_3month,
                    'fund_value_rate_6month': fund_value_rate_6month,
                    'fund_value_rate_year': fund_value_rate_year,
                    'fund_value_rate_2year': fund_value_rate_2year,
                    'fund_value_rate_3year': fund_value_rate_3year,
                    'fund_value_this_year': fund_value_this_year,
                    'fund_value_from_establish': fund_value_from_establish
                }

            url = "http://fund.eastmoney.com/%s.html" % fund_id
            yield scrapy.Request(url=url, callback=self.info_parse)

            for year in (2017, 2020):
                rn = random.random()
                url = "http://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=jjcc&code=%s&topline=50&year=%s&month=3&rt=%s" % (fund_id, str(year), str(rn))
                yield scrapy.Request(url=url, callback=self.position_parse)

    def info_parse(self, response):
        re_str = {
            'fund_name': '//div[@style="float: left"]/text()',
            'fund_id': '//span[@class="ui-num"]/text()',

            'estimated_fund_value': '//dl[@class="dataItem01"]//span[@id="gz_gsz"]/text()',
            'estimated_fund_value_change_today': '//dl[@class="dataItem01"]//span[@id="gz_gszze"]/text()',
            'estimated_fund_value_change_rate_today': '//dl[@class="dataItem01"]//span[@id="gz_gszzl"]/text()',
            'estimated_fund_value_change_rate_1month': '//dl[@class="dataItem01"]//span[text()="近1月："]/following-sibling::span[1]/text()',
            'estimated_fund_value_change_rate_1year': '//dl[@class="dataItem01"]//span[text()="近1年："]/following-sibling::span[1]/text()',

            'per_fund_value': '//dl[@class="dataItem02"]/dd[@class="dataNums"]//span[1]/text()',
            'per_fund_value_change_rate_today': '//dl[@class="dataItem02"]/dd[@class="dataNums"]//span[2]/text()',
            'per_fund_value_change_rate_3month': '//dl[@class="dataItem02"]//span[text()="近3月："]/following-sibling::span[1]/text()',
            'per_fund_value_change_rate_3year': '//dl[@class="dataItem02"]//span[text()="近3年："]/following-sibling::span[1]/text()',

            'total_fund_value': '//dl[@class="dataItem03"]/dd[@class="dataNums"]/span/text()',
            'total_fund_value_change_rate_6month': '//dl[@class="dataItem03"]//span[text()="近6月："]/following-sibling::span[1]/text()',
            'total_fund_value_change_from_establish': '//dl[@class="dataItem03"]//span[text()="成立来："]/following-sibling::span[1]/text()',

            'fund_type': '//td[text()="基金类型："]/a/text()',
            'fund_size': '//a[text()="基金规模"]/../text()',
            'fund_manager': '//td[text()="基金经理："]/a/text()',
            'fund_establish_date': '//span[@class="letterSpace01"][text()="成 立 日"]/../text()',
            'fund_manage_organization': '//span[@class="letterSpace01"][text()="管 理 人"]/../a/text()'
        }

        fund_name = response.xpath(re_str['fund_name']).extract()[0]
        fund_id = response.xpath(re_str['fund_id']).extract()[0]
        
        estimated_fund_value = response.xpath(re_str['estimated_fund_value']).extract()[0]
        estimated_fund_value_change_today = response.xpath(re_str['estimated_fund_value_change_today']).extract()[0]
        estimated_fund_value_change_rate_today = response.xpath(re_str['estimated_fund_value_change_rate_today']).extract()[0]
        estimated_fund_value_change_rate_1month = response.xpath(re_str['estimated_fund_value_change_rate_1month']).extract()[0]
        estimated_fund_value_change_rate_1year = response.xpath(re_str['estimated_fund_value_change_rate_1year']).extract()[0]

        per_fund_value = response.xpath(re_str['per_fund_value']).extract()[0]
        per_fund_value_change_rate_today = response.xpath(re_str['per_fund_value_change_rate_today']).extract()[0]
        per_fund_value_change_rate_3month = response.xpath(re_str['per_fund_value_change_rate_3month']).extract()[0]
        per_fund_value_change_rate_3year = response.xpath(re_str['per_fund_value_change_rate_3year']).extract()[0]

        total_fund_value = response.xpath(re_str['total_fund_value']).extract()[0]
        total_fund_value_change_rate_6month = response.xpath(re_str['total_fund_value_change_rate_6month']).extract()[0]
        total_fund_value_change_from_establish = response.xpath(re_str['total_fund_value_change_from_establish']).extract()[0]

        fund_type = response.xpath(re_str['fund_type']).extract()[0]
        fund_size = response.xpath(re_str['fund_size']).extract()[0]
        fund_manager = response.xpath(re_str['fund_manager']).extract()[0]
        fund_establish_date = response.xpath(re_str['fund_establish_date']).extract()[0]
        fund_manage_organization = response.xpath(re_str['fund_manage_organization']).extract()[0]

        yield {'data_type': 'fund info',
            'fund_id': fund_id,
            'fund_name': fund_name,
            'estimated_fund_value': estimated_fund_value,
            'per_fund_value': per_fund_value,
            'total_fund_value': total_fund_value,
            'estimated_fund_value_change_today': estimated_fund_value_change_today,
            'estimated_fund_value_change_rate_today': estimated_fund_value_change_rate_today,
            'estimated_fund_value_change_rate_1month': estimated_fund_value_change_rate_1month,
            'estimated_fund_value_change_rate_1year': estimated_fund_value_change_rate_1year,
            'per_fund_value_change_rate_today': per_fund_value_change_rate_today,
            'per_fund_value_change_rate_3month': per_fund_value_change_rate_3month,
            'per_fund_value_change_rate_3year': per_fund_value_change_rate_3year,
            'total_fund_value_change_rate_6month': total_fund_value_change_rate_6month,
            'total_fund_value_change_from_establish': total_fund_value_change_from_establish,
            'fund_type': fund_type,
            'fund_size': fund_size,
            'fund_manager': fund_manager,
            'fund_establish_date': fund_establish_date,
            'fund_manage_organization': fund_manage_organization
        }
    
    def position_parse(self, response):
        url_info = response.request.url.split('&')
        fund_id = ''
        for info in url_info:
            if 'code=' in info:
                fund_id = info.replace('code=','')

        fund_name = ''
        table_num = len(response.xpath('//div[@class="box"]'))

        if table_num == 0:
            return None
        
        for i in range(1, table_num + 1):
            fund_name = response.xpath('//div[@class="box"][%s]//label[@class="left"]/a/text()' % str(i)).extract()[0]
            data_date = response.xpath('//div[@class="box"][%s]//label[@class="left"]/text()' % str(i)).extract()[0].replace('\xa0\xa0', '')
            year = data_date.split('年')[0]
            season = data_date.split('年')[1].split('季度')[0]

            title_num = len(response.xpath('//div[@class="box"][%s]//thead/tr//th' % str(i)))
            
            if title_num == 7:
                stock_id = response.xpath('//div[@class="box"][%s]//tr/td[2]/a/text()|//div[@class="box"][%s]//tr/td[2]/span/text()' % (str(i), str(i))).extract()
                stock_name = response.xpath('//div[@class="box"][%s]//tr/td[3]/a/text()|//div[@class="box"][%s]//tr/td[3]/span/text()' % (str(i), str(i))).extract()
                found_value_rate = response.xpath('//div[@class="box"][%s]//tr/td[5]/text()' % str(i)).extract()
                stock_position_num = response.xpath('//div[@class="box"][%s]//tr/td[6]/text()' % str(i)).extract()
                stock_position_vlue = response.xpath('//div[@class="box"][%s]//tr/td[7]/text()' % str(i)).extract()
            elif title_num == 9:
                stock_id = response.xpath('//div[@class="box"][%s]//tr/td[2]/a/text()|//div[@class="box"][%s]//tr/td[2]/span/text()' % (str(i), str(i))).extract()
                stock_name = response.xpath('//div[@class="box"][%s]//tr/td[3]/a/text()|//div[@class="box"][%s]//tr/td[3]/span/text()' % (str(i), str(i))).extract()
                stock_price = response.xpath('//div[@class="box"][%s]//tr/td[4]/a/text()|//div[@class="box"][%s]//tr/td[4]/span/text()' % (str(i), str(i))).extract()
                stock_rate = response.xpath('//div[@class="box"][%s]//tr/td[5]/a/text()|//div[@class="box"][%s]//tr/td[5]/span/text()' % (str(i), str(i))).extract()
                found_value_rate = response.xpath('//div[@class="box"][%s]//tr/td[7]/text()' % str(i)).extract()
                stock_position_num = response.xpath('//div[@class="box"][%s]//tr/td[8]/text()' % str(i)).extract()
                stock_position_vlue = response.xpath('//div[@class="box"][%s]//tr/td[9]/text()' % str(i)).extract()

            for i in range(len(stock_id)):
                if title_num == 7 or len(stock_price) != len(stock_id):
                    yield {'data_type': 'fund position',
                        'year': year,
                        'season': season,
                        'fund_id': fund_id,
                        'fund_name': fund_name,
                        'stock_id': stock_id[i],
                        'stock_name': stock_name[i],
                        'stock_price': '--',
                        'stock_rate': '--',
                        'found_value_rate': found_value_rate[i],
                        'stock_position_num': stock_position_num[i],
                        'stock_position_vlue': stock_position_vlue[i]
                    }                
                elif title_num == 9:
                    yield {'data_type': 'fund position',
                        'year': year,
                        'season': season,
                        'fund_id': fund_id,
                        'fund_name': fund_name,
                        'stock_id': stock_id[i],
                        'stock_name': stock_name[i],
                        'stock_price': stock_price[i],
                        'stock_rate': stock_rate[i],
                        'found_value_rate': found_value_rate[i],
                        'stock_position_num': stock_position_num[i],
                        'stock_position_vlue': stock_position_vlue[i]
                    }