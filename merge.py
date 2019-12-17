import sys
from pyspark.sql import SparkSession, functions, types
from pyspark.sql import functions as f
import numpy as np
import datetime
import plotly.graph_objs as go
import pandas as pd



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

allfiles.show()
allfiles = allfiles.select(f.from_unixtime((allfiles.Arrival_time.cast('bigint')/1000+28800)).cast('timestamp').alias('Arrival_Time'),allfiles.Bus_Num,allfiles.Name)
#allfiles = allfiles.select(f.from_utc_timestamp((allfiles.Arrival_time.cast('bigint')/1000 +,"GMT")).cast('timestamp').alias('Arrival_Time'),allfiles.Bus_Num,allfiles.Name,allfiles.Arrival_time)

allfiles.show()



allfiles.createOrReplaceTempView('files')


data = spark.sql("select * from files order by Arrival_time desc")


data1 = spark.sql("select distinct Arrival_Time,Bus_Num,Name from files where Bus_Num=262 and Name="'"Paul Street"'" order by Arrival_time desc")

data1.createOrReplaceTempView('filter')

data2=spark.sql("""SELECT *, (CAST(Arrival_Time AS bigint) - CAST(lag(Arrival_Time, 1) OVER ( ORDER BY Arrival_Time) AS bigint)) as diff FROM filter""")

data2.createOrReplaceTempView('data2')

data3 = spark.sql("select *,diff-480 as delay from data2")
data3.show()

#df = data1.withColumn( "date_diff_min", (f.col("date_1").cast("long") - f.col("date_2").cast("long"))/60.).show(truncate=False)
#data.show()

#diff1 = ['2019-12-03 15:40:00','2019-12-03 15:45:00','2019-12-03 15:50:00','2019-12-03 15:55:00','2019-12-03 16:00:00','2019-12-03 16:05:00','2019-12-03 16:10:00']
x1 =list(range(50))

date_time_str = '2019-12-04 09:45:00'
Today = datetime.datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S')
date_list = [Today + datetime.timedelta(minutes=8*x) for x in range(0, 50)]
diff1=[x.strftime('%Y-%m-%d %H:%M:%S') for x in date_list]

diff2 = data2.select("Arrival_Time").rdd.flatMap(lambda x: x).sortBy(lambda x: x).collect()

x2 =list(range(50))


fig = go.Figure()
fig.add_trace(go.Scatter(x=x1, y=diff1,
                    mode='lines+markers',
                    name='Expected timing'))
fig.add_trace(go.Scatter(x=x2, y=diff2,
                    mode='lines',
                    name='Live timing'))
fig.show()


delay = data3.select("delay").rdd.flatMap(lambda x: x).collect()
delay=delay[1:]
delay = [ -x for x in delay]
bar_heights = delay
bins = [-1000, -400, 0, 400, 1000]
labels = ['Super Late', 'Late', 'Early', 'Super Ealry']

colors = {'Super Late': 'red',
          'Late': 'orange',
          'Early': 'lightgreen',
          'Super Ealry': 'darkgreen'}


df = pd.DataFrame({'y': bar_heights,
                   'x': range(len(bar_heights)),
                   'label': pd.cut(bar_heights, bins=bins, labels=labels)})
df.head()


bars = []
for label, label_df in df.groupby('label'):
    bars.append(go.Bar(x=label_df.x,
                       y=label_df.y,
                       name=label,
                       marker={'color': colors[label]}))

go.FigureWidget(data=bars).show()

# To write data in a file
#data3.coalesce(1).write.format("csv").option("header", "true").save("NewFile7.csv")
