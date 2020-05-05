# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import os
import codecs

class SweatPipeline:
    def process_item(self, item, spider):
        save_path = os.path.join('/', 'data', 'scrapy_res')
        with codecs.open(save_path, 'a', 'utf-8') as f:
            f.write(item['data'] + '\n')
