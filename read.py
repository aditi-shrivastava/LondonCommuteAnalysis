import sys
from pyspark.sql import SparkSession, functions, types


assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
spark = SparkSession.builder.appName('Read Stream').getOrCreate()
assert spark.version >= '2.3'  # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext


def main(topic):

    messages = spark.readStream.format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option("startingOffsets", "latest")  \
            .option('subscribe', 'bus').load()
    values = messages.select(messages['value'].cast('string'))
    val1 = functions.split(values.value, '--')

    xyval = values.withColumn('Bus_Station', val1.getItem(0))
    xyval = xyval.withColumn('Bus_Number', val1.getItem(1))
    xyval = xyval.withColumn('Bus_Time', val1.getItem(2))

    query = xyval.writeStream.format("csv").option("path", "/Users/rishabh/PycharmProjects/sparkwithpython/Bus10.csv").option("checkpointLocation", "/Users/rishabh/PycharmProjects/sparkwithpython/Bus10.csv").outputMode("append").start()

    # query = xyval.writeStream.outputMode("append").format("memory").queryName("query").start()
    # while (query.isActive):
    #     t.sleep(100)
    #     abc=spark.sql("select * from query where Bus_Number=53 and Bus_Station="'"Dunton Road"'"" )
    #     abc.write.csv("/Users/rishabh/PycharmProjects/sparkwithpython/streaming2.csv")


    query.awaitTermination(10000000000000);

if __name__ == '__main__':
    input_topic = sys.argv[1]
    main(input_topic)

