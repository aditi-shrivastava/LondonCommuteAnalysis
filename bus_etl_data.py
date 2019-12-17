import sys
from pyspark.sql import SparkSession, functions, types
from pyspark.sql import functions as f

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
spark = SparkSession.builder.appName('Read Stream').getOrCreate()
assert spark.version >= '2.3'  # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

df = types.StructType([
    types.StructField('Value', types.StringType()),
    types.StructField('Name', types.StringType()),
    types.StructField('Bus_Num', types.StringType()),
    types.StructField('Arrival_time', types.StringType())
])


allfiles =  spark.read.option("header","false").schema(df).csv("Bus10.csv/part-*.csv")

#allfiles = allfiles.withColumn('Arrival_time', f.to_date(allfiles.Arrival_time.cast(dataType=t.TimestampType())))

allfiles = allfiles.select(f.from_unixtime((allfiles.Arrival_time.cast('bigint')/1000+28800)).cast('timestamp').alias('Arrival_Time'),allfiles.Bus_Num,allfiles.Name)

allfiles.createOrReplaceTempView('files')

data = spark.sql("select * from files order by Arrival_time desc")

data1 = spark.sql("select distinct Arrival_Time,Bus_Num,Name from files where Bus_Num=262 and Name="'"Paul Street"'" order by Arrival_time desc")

data1.createOrReplaceTempView('filter')

data2=spark.sql("""SELECT *, (CAST(Arrival_Time AS bigint) - CAST(lag(Arrival_Time, 1) OVER ( ORDER BY Arrival_Time) AS bigint)) as diff FROM filter""")

data2.createOrReplaceTempView('data2')

data3 = spark.sql("select *,diff-480 as delay from data2")
data3.show()

data3.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/rishabh") \
    .option("dbtable", "Bus_all") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "rishabh") \
    .save()


