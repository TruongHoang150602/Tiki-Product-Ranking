from typing import Iterable
import scrapy
import re

from scrapy.http import Request
import json
from time import sleep
from urllib.parse import urlencode

class TikispiderSpider(scrapy.Spider):
    name = "tikispider"
    allowed_domains = ["tiki.vn"]
    start_urls = ["https://api.tiki.vn/raiden/v2/menu-config?platform=desktop"]
    custom_settings = {
        'CONCURRENT_REQUESTS': 10,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 4,
    }
    def parse(self, response):
        category_id_list = re.findall('/c(\d+)"', response.text)

        for category_id in category_id_list:
            page = 1
            while page <= 50:
                url_category = f'https://tiki.vn/api/personalish/v1/blocks/listings?category={category_id}&page={page}'
                page+=1
                yield response.follow(url=url_category, callback=self.parse_page)

    def parse_page(self, response):
        product_id = re.findall('{"id":(\d+)', response.text)
        for id in product_id:
            url_product = f'https://tiki.vn/api/v2/products/{id}'
            yield response.follow(url=url_product, callback=self.parse_product)            

    def parse_product(self, response):
        if response.status == 200:
            try:
                data_product = json.loads(response.text)
            except json.JSONDecodeError as e:
                with open("crawler.log", mode="a") as log:
                    log.write(f"JSON Decode Error: {e}: {response}\n")
            else:
                yield data_product
        else:
            with open("crawler.log", mode="a") as log:
                log.write(f'Status Code {response.status}')
            return




        
