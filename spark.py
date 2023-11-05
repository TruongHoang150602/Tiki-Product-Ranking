import pyspark
from pyspark.sql import SparkSession
from pyspark.streaming import KafkaUtils
import json


def main():
    # Tạo SparkSession
    spark = SparkSession.builder.appName("KafkaToHadoop").getOrCreate()

    # Tạo KafkaUtils
    kafkaUtils = KafkaUtils.createDirectStream(
        spark,
        ['product-tiki'],
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    # Xử lý dữ liệu trong DStream
    dstream = kafkaUtils.map(lambda x: x['data'])

    # Lưu dữ liệu vào hadoop
    dstream.saveAsNewAPIHadoopFile(
        'hdfs://localhost:9000/tiki',
        'org.apache.hadoop.io.Text',
        'org.apache.hadoop.io.Text',
        'org.apache.hadoop.mapreduce.lib.output.TextOutputFormat'
    )

    # Chạy chương trình
    spark.stop()


if __name__ == '__main__':
    main()