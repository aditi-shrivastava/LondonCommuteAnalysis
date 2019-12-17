import sys
import json
from pyspark import SparkFiles
import requests

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
import pyspark.sql.functions as f
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('Infrastructure').config('spark.driver.extraClassPath','postgresql-42.2.8.jar').getOrCreate()
assert spark.version >= '2.4'  # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

app_id = '8c71bcb9'
app_key = '3cbcc5c0f376eea45a0f9be826e07055'

schema_BikePark = types.StructType([
    types.StructField('id', types.StringType()),
    types.StructField('name', types.StringType()),
    types.StructField('lat', types.FloatType()),
    types.StructField('lon', types.FloatType()),
    types.StructField('ndocks', types.IntegerType()),
    types.StructField('nbike', types.IntegerType()),
    types.StructField('nfree', types.IntegerType()),
])

schema_CarPark = types.StructType([
    types.StructField('id', types.StringType()),
    types.StructField('lat', types.FloatType()),
    types.StructField('lon', types.FloatType()),
])

schema_CarOccupancy = types.StructType([
    types.StructField('id', types.StringType()),
    types.StructField('name', types.StringType()),
    types.StructField('baytype', types.StringType()),
    types.StructField('baycount', types.IntegerType()),
    types.StructField('occupied', types.IntegerType()),
    types.StructField('free', types.IntegerType()),
])


def main():
    carpark()
    bike()


def bike():
    url_BikePark = "https://api.tfl.gov.uk/BikePoint"
    BikePark = json.loads(requests.get(url_BikePark).content.decode('utf-8'))
    df_BikeOccupancy = spark.createDataFrame([], schema=schema_BikePark)
    for data in BikePark:
        id = data['id']
        name = data['commonName']
        lat = data['lat']
        lon = data['lon']
        nbike = int(data['additionalProperties'][6]['value'])
        nfree = int(data['additionalProperties'][7]['value'])
        ndocks = int(data['additionalProperties'][8]['value'])
        newRow = spark.createDataFrame([(id, name, lat, lon, ndocks, nbike, nfree)], schema_BikePark)
        df_BikeOccupancy = df_BikeOccupancy.union(newRow)


    df_BikeOccupancy.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "bikepark") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .save()


def carpark():
    url_CarPark = "https://api.tfl.gov.uk/Place/Type/CarPark"
    url_Occupancy = "https://api.tfl.gov.uk/Occupancy/CarPark"  # + '?app_id=' + app_id + '&' + 'app_key=' + app_key

    CarPark = json.loads(requests.get(url_CarPark).content.decode('utf-8'))
    CarOccupancy = json.loads(requests.get(url_Occupancy).content.decode('utf-8'))

    df_CarPark = spark.createDataFrame([], schema=schema_CarPark)
    for data in CarPark:
        id = data['id']
        lat = data['lat']
        lon = data['lon']
        newRow = spark.createDataFrame([(id, lat, lon)], schema_CarPark)
        df_CarPark = df_CarPark.union(newRow)
    # print(df_CarPark.show(60,False))

    df_CarOccupancy = spark.createDataFrame([], schema=schema_CarOccupancy)
    for data in CarOccupancy:
        for i in range(len(data['bays'])):
            id = data['id']
            name = data['name']
            baytype = data['bays'][i]['bayType']
            baycount = data['bays'][i]['bayCount']
            occupied = data['bays'][i]['free']
            free = data['bays'][i]['occupied']
            newRow = spark.createDataFrame([(id, name, baytype, baycount, occupied, free)], schema_CarOccupancy)
            df_CarOccupancy = df_CarOccupancy.union(newRow)


    final = df_CarOccupancy.join(df_CarPark, ['id'])

    split_name = f.split(final['name'], '[(]')
    final = final.withColumn('namesplit', split_name.getItem(0))
    final = final.select('id','namesplit','baytype','baycount','occupied','free','lat','lon')
    final = final.withColumnRenamed("namesplit","name")
    print(final.show(200, False))

    final.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "carpark") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .save()

if __name__ == '__main__':

    main()

'''
Below code is written to find specific key.
for i in range(len(data['additionalProperties'])):
    electric = data['additionalProperties'][i]['key']
    if electric == 'CarElectricalChargingPoints':
        value = data['additionalProperties'][i]['value']
        print(electric)
        print(value)
'''
