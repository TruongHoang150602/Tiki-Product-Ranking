import scrapy
import json
from kafka import KafkaProducer


class KafkaPipeline():
    def __init__(self):
        super().__init__()
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def process_item(self, item, spider):
        # Xử lý dữ liệu trước khi gửi lên kafka
        # item['data'] = item['data'].replace('"', '')

        # Gửi dữ liệu lên topic kafka
        self.producer.send('product-tiki', item)
        return item

