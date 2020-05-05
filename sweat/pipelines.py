# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import os
import codecs

class SweatPipeline:
    def __init__(self):
        self.first_flag = {
            'fund list': True,
            'fund info': True,
            'fund position': True
        }

    def process_item(self, item, spider):
        flag = False
        save_path = None

        if item['data_type'] == 'fund list':
            save_path = os.path.join('/', 'data', 'fund_list')
            info_name = ['fund_id','fund_name','date','per_fund_vale','total_fund_value','fund_value_rate_week','fund_value_rate_month','fund_value_rate_3month','fund_value_rate_6month',
                            'fund_value_rate_year','fund_value_rate_2year','fund_value_rate_3year','fund_value_this_year','fund_value_from_establish']
            if self.first_flag[item['data_type']]:
                flag = True
                self.first_flag[item['data_type']] = False
        elif item['data_type'] == 'fund info':
            save_path = os.path.join('/', 'data', 'fund_info')
            info_name = ['fund_id','fund_name','estimated_fund_value','per_fund_value','total_fund_value','estimated_fund_value_change_today','estimated_fund_value_change_rate_today','estimated_fund_value_change_rate_1month',
                            'estimated_fund_value_change_rate_1year','per_fund_value_change_rate_today','per_fund_value_change_rate_3month','per_fund_value_change_rate_3year','total_fund_value_change_rate_6month','total_fund_value_change_from_establish',
                            'fund_type','fund_size','fund_manager','fund_establish_date','fund_manage_organization']
            if self.first_flag[item['data_type']]:
                flag = True
                self.first_flag[item['data_type']] = False
        elif item['data_type'] == 'fund position':
            save_path = os.path.join('/', 'data', 'fund_pos')
            info_name = ['year','season','fund_id','fund_name','stock_id','stock_name','stock_price','stock_rate','found_value_rate', 'stock_position_num', 'stock_position_vlue']
            if self.first_flag[item['data_type']]:
                flag = True
                self.first_flag[item['data_type']] = False

        if save_path is None:
            print('------------------------------')
            print(item)
            print('------------------------------')
        else:
            with codecs.open(save_path, 'a', 'utf-8') as f:
                if flag:
                    f.write('|'.join(info_name) + '\n')
                
                write_info = []
                for name in info_name:
                    write_info.append(item[name])
                f.write('|'.join(write_info) + '\n')