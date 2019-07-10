import os
from pyspark.streaming import StreamingContext
from pyspark import SparkContext
from pyspark.streaming.kafka import KafkaUtils
import json
from kafka import KafkaProducer
from pykafka import KafkaClient


producer_py = KafkaProducer(bootstrap_servers='localhost:9092')

def push(status):
    client = KafkaClient(hosts="localhost:9092")
    topic = client.topics['streaming_data_backward']
    for stx in status:
	    with topic.get_sync_producer() as producer:
		    producer.produce(json.dumps(stx).encode('utf-8'))

# def process(time, rdd):
#     print("========= %s =========" % str(time))
#     try:
#
#         # Convert RDD[String] to RDD[Row] to DataFrame
#         rowRdd = rdd.map(lambda w: Row(date = datetime.datetime.strptime(w[0], '%Y-%m-%d'), distance = float(w[1])))
#         DataFrame = spark.createDataFrame(rowRdd).orderBy('date', ascending=True)
#
#         # w = (Window.orderBy(f.col("date").cast('long')).rowsBetween(0, 2)) #should be modified
#         # DataFrame = DataFrame.withColumn('rolling_average', f.avg("distance").over(w))
#         DataFrame.show()
#     except:
#         pass


def extractor(datetime):
    return datetime.rsplit(' ', 1)[0]


def event(raw):
    item = list(raw[1].items())[0]
    return (extractor(item[0]), (1, float(item[1])))


if __name__ == "__main__":
    os.environ['SPARK_HOME']="/home/antonio/Desktop/spark/"
    os.environ['PYSPARK_SUBMIT_ARGS'] = ' --packages org.apache.spark:spark-streaming-kafka-0-8-assembly_2.11:2.3.0 pyspark-shell'
    os.environ['JAVA_HOME'] = "/usr/lib/jvm/java-8-openjdk-amd64/jre/"
    sc = SparkContext("local[*]", "MY-topic")
    ssc = StreamingContext(sc, 3)
    new_data = KafkaUtils.createDirectStream(ssc,
                                            ['streaming_data_forward'],
                                            kafkaParams = {"metadata.broker.list": "localhost:9092"},
                                            valueDecoder=lambda s: json.loads(s.decode('ascii')))

    new_data = new_data.map(event)\
                       .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
                       .map(lambda r: (r[0], float(r[1][1]) / r[1][0]))
    new_data.foreachRDD(lambda rdd: rdd.foreachPartition(push))
    ssc.start()
    ssc.awaitTermination()





