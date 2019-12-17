import sys
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import expr
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, types
import pyspark.sql.functions as f
import psycopg2
spark = SparkSession.builder.appName('Accident Analysis').config('spark.driver.extraClassPath','postgresql-42.2.8.jar').getOrCreate()
assert spark.version >= '2.4'  # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

schema_attendant = types.StructType([
    types.StructField('arefno', types.StringType()),
    types.StructField('borough', types.StringType()),
    types.StructField('boro', types.IntegerType()),
    types.StructField('easting', types.LongType()),
    types.StructField('northing', types.LongType()),
    types.StructField('location', types.StringType()),
    types.StructField('accidentseverity', types.StringType()),
    types.StructField('numofcasualties', types.IntegerType()),
    types.StructField('numofvehicles', types.IntegerType()),
    types.StructField('accidentdate', types.StringType()),  # Consider using DateType here
    types.StructField('day', types.StringType()),
    types.StructField('time', types.StringType()),
    types.StructField('highway', types.StringType()),
    types.StructField('roadclass1', types.StringType()),
    types.StructField('roadnum1', types.LongType()),
    types.StructField('roadtype', types.StringType()),
    types.StructField('speedlimit', types.StringType()),
    types.StructField('junctiondetail', types.StringType()),
    types.StructField('junctioncontrol', types.StringType()),
    types.StructField('roadclass2', types.StringType()),
    types.StructField('roadnum2', types.LongType()),
    types.StructField('pedcrossingdecoded', types.StringType()),
    types.StructField('lightconditionsbanded', types.StringType()),
    types.StructField('weather', types.StringType()),
    types.StructField('roadsurface', types.StringType()),
    types.StructField('specialconditions', types.StringType()),
    types.StructField('cwhazard', types.StringType()),
])

schema_vehicle = types.StructType([
    types.StructField('arefno', types.StringType()),
    types.StructField('borough', types.StringType()),
    types.StructField('boro', types.IntegerType()),
    types.StructField('easting', types.LongType()),
    types.StructField('northing', types.LongType()),
    types.StructField('vehicleref', types.IntegerType()),
    types.StructField('vehicletype', types.StringType()),
    types.StructField('vehicletypebanded', types.StringType()),
    types.StructField('vehiclemanoeuvres', types.StringType()),
    types.StructField('vehicleskidding', types.StringType()),
    types.StructField('restrictedlane', types.StringType()),
    types.StructField('junctionlocation', types.StringType()),
    types.StructField('objectincw', types.StringType()),
    types.StructField('vehicleleavingcw', types.StringType()),
    types.StructField('vehicleoffcw', types.StringType()),
    types.StructField('vehicleimpact', types.StringType()),
    types.StructField('vjnypurpdecoded', types.StringType()),
    types.StructField('driversex', types.StringType()),
    types.StructField('driverage', types.IntegerType()),
    types.StructField('driveragebanded', types.StringType()),
])

schema_casualty = types.StructType([
    types.StructField('arefno', types.StringType()),
    types.StructField('borough', types.StringType()),
    types.StructField('boro', types.IntegerType()),
    types.StructField('easting', types.LongType()),
    types.StructField('northing', types.LongType()),
    types.StructField('crefno', types.IntegerType()),
    types.StructField('casualtyclass', types.StringType()),
    types.StructField('casualtysex', types.StringType()),
    types.StructField('casualtyagebanded', types.StringType()),
    types.StructField('casualtyage', types.IntegerType()),
    types.StructField('numofcasualty', types.IntegerType()),
    types.StructField('casualtyseverity', types.StringType()),
    types.StructField('pedlocation', types.StringType()),
    types.StructField('pedmovement', types.StringType()),
    types.StructField('modeoftravel', types.StringType()),
])


def main():
    accident()


def accident():
    file_attendent_14 = 'data/2014-gla-data-extract-attendant.csv'
    file_casualty_14 = 'data/2014-gla-data-extract-casualty.csv'

    file_attendent_15 = 'data/2015-gla-data-extract-attendant.csv'
    file_casualty_15 = 'data/2015-gla-data-extract-casualty.csv'

    file_attendent_16 = 'data/2016-gla-data-extract-attendant.csv'
    file_casualty_16 = 'data/2016-gla-data-extract-casualty.csv'

    file_attendent_17 = 'data/2017-data-attendant.csv'
    file_casualty_17 = 'data/2017-data-casualty.csv'

    file_attendent_18 = 'data/2018-data-files-attendant.csv'
    file_casualty_18 = 'data/2018-data-files-casualty.csv'

    data_attendent_14 = spark.read.csv(file_attendent_14, header='true', schema=schema_attendant)
    data_attendent_14.createOrReplaceTempView('data_attendent_14')

    data_casualty_14 = spark.read.csv(file_casualty_14, header='true', schema=schema_casualty)
    data_casualty_14.createOrReplaceTempView('data_casualty_14')

    data_attendent_15 = spark.read.csv(file_attendent_15, header='true', schema=schema_attendant)
    data_attendent_15.createOrReplaceTempView('data_attendent_15')

    data_casualty_15 = spark.read.csv(file_casualty_15, header='true', schema=schema_casualty)
    data_casualty_15.createOrReplaceTempView('data_casualty_15')

    data_attendent_16 = spark.read.csv(file_attendent_16, header='true', schema=schema_attendant)
    data_attendent_16.createOrReplaceTempView('data_attendent_16')

    data_casualty_16 = spark.read.csv(file_casualty_16, header='true', schema=schema_casualty)
    data_casualty_16.createOrReplaceTempView('data_casualty_16')

    data_attendent_17 = spark.read.csv(file_attendent_17, header='true', schema=schema_attendant)
    data_attendent_17.createOrReplaceTempView('data_attendent_17')

    data_casualty_17 = spark.read.csv(file_casualty_17, header='true', schema=schema_casualty)
    data_casualty_17.createOrReplaceTempView('data_casualty_17')

    data_attendent_18 = spark.read.csv(file_attendent_18, header='true', schema=schema_attendant)
    data_attendent_18.createOrReplaceTempView('data_attendent_18')

    data_casualty_18 = spark.read.csv(file_casualty_18, header='true', schema=schema_casualty)
    data_casualty_18.createOrReplaceTempView('data_casualty_18')

    """""""""""""""""""""""""""""""""""""""""""""""
    ETL STARTS HERE!!!!!
    """""""""""""""""""""""""""""""""""""""""""""""
    data_attendent_14 = spark.sql(
        "SELECT arefno AS id,borough,easting,northing,location,accidentseverity,numofcasualties,accidentdate,day,"
        "time,weather FROM data_attendent_14").cache()

    split_severity = f.split(data_attendent_14['accidentseverity'], ' ')
    data_attendent_14 = data_attendent_14.withColumn('severity', split_severity.getItem(1)).withColumn('severitynum',
                                                                                                       split_severity.getItem(
                                                                                                           0))

    split_weather = f.split(data_attendent_14['weather'], '^[^\s]*\s')
    data_attendent_14 = data_attendent_14.withColumn('weathersplit', split_weather.getItem(1))

    data_attendent_14 = data_attendent_14.filter(data_attendent_14.weathersplit != 'Unknown')

    data_attendent_14 = data_attendent_14.withColumn('time_new',
                                                     f.concat(f.substring(data_attendent_14['time'], 2, 2), f.lit(":"),
                                                              f.substring(data_attendent_14['time'], 4, 2)))

    split_time = f.split(data_attendent_14['time'], "'")
    data_attendent_14 = data_attendent_14.withColumn('time_int', split_time.getItem(1).cast(IntegerType()))

    data_attendent_14 = data_attendent_14.withColumn("timebanded",
                        expr("case when time_int BETWEEN 0 AND 400 then '00-04' " +
                             "when time_int BETWEEN 401 AND 800 then '04-08' " +
                             "when time_int BETWEEN 801 AND 1200 then '08-12' " +
                             "when time_int BETWEEN 1201 AND 1600 then '12-16' " +
                             "when time_int BETWEEN 1601 AND 2000 then '16-20' " +
                             "when time_int BETWEEN 2001 AND 2400 then '20-24' " +
                             "else 'Unknown' end"))

    data_attendent_14 = data_attendent_14.withColumn("daynum",
                                                     expr("case when day like 'Monday%' then 1 " +
                                                          "when day like 'Tuesday%' then 2 " +
                                                          "when day like 'Wednesday%' then 3 " +
                                                          "when day like 'Thursday%' then 4 " +
                                                          "when day like 'Friday%' then 5 " +
                                                          "when day like 'Saturday%' then 6 " +
                                                          "when day like 'Sunday%' then 7 " +
                                                          "else 'Unknown' end"))

    data_attendent_14.createOrReplaceTempView('data_attendent_14')

    data_casualty_14 = spark.sql(
        "SELECT arefno AS id,borough,casualtyclass,casualtysex,casualtyagebanded,casualtyseverity,modeoftravel FROM"
        " data_casualty_14").cache()

    split_class = f.split(data_casualty_14['casualtysex'], '^[^\s]*\s')
    data_casualty_14 = data_casualty_14.withColumn('csex', split_class.getItem(1))

    split_csex = f.split(data_casualty_14['casualtyclass'], '^[^\s]*\s')
    data_casualty_14 = data_casualty_14.withColumn('class', split_csex.getItem(1))

    split_cseverity = f.split(data_casualty_14['casualtyseverity'], '^[^\s]*\s')
    data_casualty_14 = data_casualty_14.withColumn('cseverity', split_cseverity.getItem(1))

    split_travel = f.split(data_casualty_14['modeoftravel'], '^[^\s]*\s')
    data_casualty_14 = data_casualty_14.withColumn('travel', split_travel.getItem(1))

    data_casualty_14.createOrReplaceTempView('data_casualty_14')

    table_sexage_14 = spark.sql("SELECT casualtyagebanded,csex,borough,cseverity from data_casualty_14 where cseverity IN \
                             ('Serious','Fatal') AND casualtyagebanded != 'Unknown'")

    table_sexage_14.createOrReplaceTempView('table_sexage_14')

    report_borough_14 = spark.sql(
        "SELECT casualtyagebanded AS age,csex,count(csex) AS count,borough FROM table_sexage_14 \
         GROUP BY casualtyagebanded,csex,borough,cseverity ORDER BY casualtyagebanded")

    report_borough_14.createOrReplaceTempView('report_borough_14')

    table_borough_14 = spark.sql(
        "SELECT A.id,A.borough,A.severity AS accidentseverity,A.numofcasualties,C.travel AS modeoftravel FROM"
        " data_attendent_14 as A INNER JOIN data_casualty_14 as C on A.id = C.id")
    table_borough_14.createOrReplaceTempView('table_borough_14')
    """""""""""""""""""""""""""""""""""""""""""""""
    14 ENDS HERE!!!!!
    """""""""""""""""""""""""""""""""""""""""""""""

    data_attendent_15 = spark.sql(
        "SELECT arefno AS id,borough,easting,northing,location,accidentseverity,numofcasualties,accidentdate,day,"
        "time,weather FROM data_attendent_15").cache()

    split_severity = f.split(data_attendent_15['accidentseverity'], ' ')
    data_attendent_15 = data_attendent_15.withColumn('severity', split_severity.getItem(1)).withColumn('severitynum',
                                                                                                       split_severity.getItem(
                                                                                                           0))
    split_weather = f.split(data_attendent_15['weather'], '^[^\s]*\s')
    data_attendent_15 = data_attendent_15.withColumn('weathersplit', split_weather.getItem(1))

    data_attendent_15 = data_attendent_15.filter(data_attendent_15.weathersplit != 'Unknown')

    data_attendent_15 = data_attendent_15.withColumn('time_new',
                                                     f.concat(f.substring(data_attendent_15['time'], 2, 2), f.lit(":"),
                                                              f.substring(data_attendent_15['time'], 4, 2)))

    split_time = f.split(data_attendent_15['time'], "'")
    data_attendent_15 = data_attendent_15.withColumn('time_int', split_time.getItem(1).cast(IntegerType()))

    data_attendent_15 = data_attendent_15.withColumn("timebanded",
                                                     expr("case when time_int BETWEEN 0 AND 400 then '00-04' " +
                                                          "when time_int BETWEEN 401 AND 800 then '04-08' " +
                                                          "when time_int BETWEEN 801 AND 1200 then '08-12' " +
                                                          "when time_int BETWEEN 1201 AND 1600 then '12-16' " +
                                                          "when time_int BETWEEN 1601 AND 2000 then '16-20' " +
                                                          "when time_int BETWEEN 2001 AND 2400 then '20-24' " +
                                                          "else 'Unknown' end"))

    data_attendent_15 = data_attendent_15.withColumn("daynum",
                                                     expr("case when day like 'Monday%' then 1 " +
                                                          "when day like 'Tuesday%' then 2 " +
                                                          "when day like 'Wednesday%' then 3 " +
                                                          "when day like 'Thursday%' then 4 " +
                                                          "when day like 'Friday%' then 5 " +
                                                          "when day like 'Saturday%' then 6 " +
                                                          "when day like 'Sunday%' then 7 " +
                                                          "else 'Unknown' end"))

    data_attendent_15.createOrReplaceTempView('data_attendent_15')

    data_casualty_15 = spark.sql(
        "SELECT arefno AS id,borough,casualtyclass,casualtysex,casualtyagebanded,casualtyseverity,modeoftravel FROM"
        " data_casualty_15").cache()

    split_class = f.split(data_casualty_15['casualtysex'], '^[^\s]*\s')
    data_casualty_15 = data_casualty_15.withColumn('csex', split_class.getItem(1))

    split_csex = f.split(data_casualty_15['casualtyclass'], '^[^\s]*\s')
    data_casualty_15 = data_casualty_15.withColumn('class', split_csex.getItem(1))

    split_cseverity = f.split(data_casualty_15['casualtyseverity'], '^[^\s]*\s')
    data_casualty_15 = data_casualty_15.withColumn('cseverity', split_cseverity.getItem(1))

    split_travel = f.split(data_casualty_15['modeoftravel'], '^[^\s]*\s')
    data_casualty_15 = data_casualty_15.withColumn('travel', split_travel.getItem(1))

    data_casualty_15.createOrReplaceTempView('data_casualty_15')

    table_sexage_15 = spark.sql("SELECT casualtyagebanded,csex,borough,cseverity from data_casualty_15 where cseverity IN \
                             ('Serious','Fatal') AND casualtyagebanded != 'Unknown'")

    table_sexage_15.createOrReplaceTempView('table_sexage_15')

    report_borough_15 = spark.sql(
        "SELECT casualtyagebanded AS age,csex,count(csex) AS count,borough FROM table_sexage_15 \
         GROUP BY casualtyagebanded,csex,borough,cseverity ORDER BY casualtyagebanded")

    report_borough_15.createOrReplaceTempView('report_borough_15')

    table_borough_15 = spark.sql(
        "SELECT A.id,A.borough,A.severity AS accidentseverity,A.numofcasualties,C.travel AS modeoftravel FROM"
        " data_attendent_15 as A INNER JOIN data_casualty_15 as C on A.id = C.id")
    table_borough_15.createOrReplaceTempView('table_borough_15')
    """""""""""""""""""""""""""""""""""""""""""""""
    15 ENDS HERE!!!!!
    """""""""""""""""""""""""""""""""""""""""""""""

    data_attendent_16 = spark.sql(
        "SELECT arefno AS id,borough,easting,northing,location,accidentseverity,numofcasualties,accidentdate,day,"
        "time,weather FROM data_attendent_16").cache()

    split_severity = f.split(data_attendent_16['accidentseverity'], ' ')
    data_attendent_16 = data_attendent_16.withColumn('severity', split_severity.getItem(1)).withColumn('severitynum',
                                                                                                       split_severity.getItem(
                                                                                                           0))

    split_weather = f.split(data_attendent_16['weather'], '^[^\s]*\s')
    data_attendent_16 = data_attendent_16.withColumn('weathersplit', split_weather.getItem(1))

    data_attendent_16 = data_attendent_16.filter(data_attendent_16.weathersplit != 'Unknown')

    split_time = f.split(data_attendent_16['time'], ":")
    data_attendent_16 = data_attendent_16.withColumn('time_int',
                                                     f.concat(split_time.getItem(0), split_time.getItem(1)).cast(
                                                         IntegerType()))

    data_attendent_16 = data_attendent_16.withColumn("timebanded",
                                                     expr("case when time_int BETWEEN 0 AND 400 then '00-04' " +
                                                          "when time_int BETWEEN 401 AND 800 then '04-08' " +
                                                          "when time_int BETWEEN 801 AND 1200 then '08-12' " +
                                                          "when time_int BETWEEN 1201 AND 1600 then '12-16' " +
                                                          "when time_int BETWEEN 1601 AND 2000 then '16-20' " +
                                                          "when time_int BETWEEN 2001 AND 2400 then '20-24' " +
                                                          "else 'Unknown' end"))

    data_attendent_16 = data_attendent_16.withColumn("daynum",
                                                     expr("case when day like 'Monday%' then 1 " +
                                                          "when day like 'Tuesday%' then 2 " +
                                                          "when day like 'Wednesday%' then 3 " +
                                                          "when day like 'Thursday%' then 4 " +
                                                          "when day like 'Friday%' then 5 " +
                                                          "when day like 'Saturday%' then 6 " +
                                                          "when day like 'Sunday%' then 7 " +
                                                          "else 'Unknown' end"))

    data_attendent_16.createOrReplaceTempView('data_attendent_16')

    data_casualty_16 = spark.sql(
        "SELECT arefno AS id,borough,casualtyclass,casualtysex,casualtyagebanded,casualtyseverity,modeoftravel FROM"
        " data_casualty_16").cache()

    split_class = f.split(data_casualty_16['casualtysex'], '^[^\s]*\s')
    data_casualty_16 = data_casualty_16.withColumn('csex', split_class.getItem(1))

    split_csex = f.split(data_casualty_16['casualtyclass'], '^[^\s]*\s')
    data_casualty_16 = data_casualty_16.withColumn('class', split_csex.getItem(1))

    split_cseverity = f.split(data_casualty_16['casualtyseverity'], '^[^\s]*\s')
    data_casualty_16 = data_casualty_16.withColumn('cseverity', split_cseverity.getItem(1))

    split_travel = f.split(data_casualty_16['modeoftravel'], '^[^\s]*\s')
    data_casualty_16 = data_casualty_16.withColumn('travel', split_travel.getItem(1))

    data_casualty_16.createOrReplaceTempView('data_casualty_16')

    table_sexage_16 = spark.sql("SELECT casualtyagebanded,csex,borough,cseverity from data_casualty_16 where cseverity IN \
                             ('Serious','Fatal') AND casualtyagebanded != 'Unknown'")

    table_sexage_16.createOrReplaceTempView('table_sexage_16')

    report_borough_16 = spark.sql(
        "SELECT casualtyagebanded AS age,csex,count(csex) AS count,borough FROM table_sexage_16 \
         GROUP BY casualtyagebanded,csex,borough,cseverity ORDER BY casualtyagebanded")

    report_borough_16.createOrReplaceTempView('report_borough_16')

    table_borough_16 = spark.sql(
        "SELECT A.id,A.borough,A.severity AS accidentseverity,A.numofcasualties,C.travel AS modeoftravel FROM"
        " data_attendent_16 as A INNER JOIN data_casualty_16 as C on A.id = C.id")
    table_borough_16.createOrReplaceTempView('table_borough_16')
    """""""""""""""""""""""""""""""""""""""""""""""
    16 ENDS HERE!!!!!
    """""""""""""""""""""""""""""""""""""""""""""""

    data_attendent_17 = spark.sql(
        "SELECT arefno AS id,borough,easting,northing,location,accidentseverity,numofcasualties,accidentdate,day,"
        "time,weather FROM data_attendent_17").cache()

    split_severity = f.split(data_attendent_17['accidentseverity'], ' ')
    data_attendent_17 = data_attendent_17.withColumn('severity', split_severity.getItem(1)).withColumn('severitynum',
                                                                                                       split_severity.getItem(
                                                                                                           0))
    split_weather = f.split(data_attendent_17['weather'], '^[^\s]*\s')
    data_attendent_17 = data_attendent_17.withColumn('weathersplit', split_weather.getItem(1))

    data_attendent_17 = data_attendent_17.filter(data_attendent_17.weathersplit != 'Unknown')

    data_attendent_17 = data_attendent_17.withColumn('time_new',
                                                     f.concat(f.substring(data_attendent_17['time'], 2, 2), f.lit(":"),
                                                              f.substring(data_attendent_17['time'], 4, 2)))

    split_time = f.split(data_attendent_17['time'], "'")
    data_attendent_17 = data_attendent_17.withColumn('time_int', split_time.getItem(1).cast(IntegerType()))

    data_attendent_17 = data_attendent_17.withColumn("timebanded",
                                                     expr("case when time_int BETWEEN 0 AND 400 then '00-04' " +
                                                          "when time_int BETWEEN 401 AND 800 then '04-08' " +
                                                          "when time_int BETWEEN 801 AND 1200 then '08-12' " +
                                                          "when time_int BETWEEN 1201 AND 1600 then '12-16' " +
                                                          "when time_int BETWEEN 1601 AND 2000 then '16-20' " +
                                                          "when time_int BETWEEN 2001 AND 2400 then '20-24' " +
                                                          "else 'Unknown' end"))

    data_attendent_17 = data_attendent_17.withColumn("daynum",
                                                     expr("case when day like 'Monday%' then 1 " +
                                                          "when day like 'Tuesday%' then 2 " +
                                                          "when day like 'Wednesday%' then 3 " +
                                                          "when day like 'Thursday%' then 4 " +
                                                          "when day like 'Friday%' then 5 " +
                                                          "when day like 'Saturday%' then 6 " +
                                                          "when day like 'Sunday%' then 7 " +
                                                          "else 'Unknown' end"))

    data_attendent_17.createOrReplaceTempView('data_attendent_17')

    data_casualty_17 = spark.sql(
        "SELECT arefno AS id,borough,casualtyclass,casualtysex,casualtyagebanded,casualtyseverity,modeoftravel FROM"
        " data_casualty_17").cache()

    split_class = f.split(data_casualty_17['casualtysex'], '^[^\s]*\s')
    data_casualty_17 = data_casualty_17.withColumn('csex', split_class.getItem(1))

    split_csex = f.split(data_casualty_17['casualtyclass'], '^[^\s]*\s')
    data_casualty_17 = data_casualty_17.withColumn('class', split_csex.getItem(1))

    split_cseverity = f.split(data_casualty_17['casualtyseverity'], '^[^\s]*\s')
    data_casualty_17 = data_casualty_17.withColumn('cseverity', split_cseverity.getItem(1))

    split_travel = f.split(data_casualty_17['modeoftravel'], '^[^\s]*\s')
    data_casualty_17 = data_casualty_17.withColumn('travel', split_travel.getItem(1))

    data_casualty_17.createOrReplaceTempView('data_casualty_17')

    table_sexage_17 = spark.sql("SELECT casualtyagebanded,csex,borough,cseverity from data_casualty_17 where cseverity IN \
                             ('Serious','Fatal') AND casualtyagebanded != 'Unknown'")

    table_sexage_17.createOrReplaceTempView('table_sexage_17')

    report_borough_17 = spark.sql(
        "SELECT casualtyagebanded AS age,csex,count(csex) AS count,borough FROM table_sexage_17 \
         GROUP BY casualtyagebanded,csex,borough,cseverity ORDER BY casualtyagebanded")

    report_borough_17.createOrReplaceTempView('report_borough_17')

    table_borough_17 = spark.sql(
        "SELECT A.id,A.borough,A.severity AS accidentseverity,A.numofcasualties,C.travel AS modeoftravel FROM"
        " data_attendent_17 as A INNER JOIN data_casualty_17 as C on A.id = C.id")
    table_borough_17.createOrReplaceTempView('table_borough_17')
    """""""""""""""""""""""""""""""""""""""""""""""
    17 ENDS HERE!!!!!
    """""""""""""""""""""""""""""""""""""""""""""""

    data_attendent_18 = spark.sql(
        "SELECT arefno AS id,borough,easting,northing,location,accidentseverity,numofcasualties,accidentdate,day,"
        "time,weather FROM data_attendent_18").cache()

    split_severity = f.split(data_attendent_18['accidentseverity'], ' ')
    data_attendent_18 = data_attendent_18.withColumn('severity', split_severity.getItem(1)).withColumn('severitynum',
                                                                                                       split_severity.getItem(
                                                                                                           0))

    split_weather = f.split(data_attendent_18['weather'], '^[^\s]*\s')
    data_attendent_18 = data_attendent_18.withColumn('weathersplit', split_weather.getItem(1))

    data_attendent_18 = data_attendent_18.filter(data_attendent_18.weathersplit != 'Unknown')

    data_attendent_18 = data_attendent_18.withColumn('time_new',
                                                     f.concat(f.substring(data_attendent_18['time'], 2, 2), f.lit(":"),
                                                              f.substring(data_attendent_18['time'], 4, 2)))

    split_time = f.split(data_attendent_18['time'], "'")
    data_attendent_18 = data_attendent_18.withColumn('time_int', split_time.getItem(1).cast(IntegerType()))

    data_attendent_18 = data_attendent_18.withColumn("timebanded",
                                                     expr("case when time_int BETWEEN 0 AND 400 then '00-04' " +
                                                          "when time_int BETWEEN 401 AND 800 then '04-08' " +
                                                          "when time_int BETWEEN 801 AND 1200 then '08-12' " +
                                                          "when time_int BETWEEN 1201 AND 1600 then '12-16' " +
                                                          "when time_int BETWEEN 1601 AND 2000 then '16-20' " +
                                                          "when time_int BETWEEN 2001 AND 2400 then '20-24' " +
                                                          "else 'Unknown' end"))

    data_attendent_18 = data_attendent_18.withColumn("daynum",
                                                     expr("case when day like 'Monday%' then 1 " +
                                                          "when day like 'Tuesday%' then 2 " +
                                                          "when day like 'Wednesday%' then 3 " +
                                                          "when day like 'Thursday%' then 4 " +
                                                          "when day like 'Friday%' then 5 " +
                                                          "when day like 'Saturday%' then 6 " +
                                                          "when day like 'Sunday%' then 7 " +
                                                          "else 'Unknown' end"))

    data_attendent_18.createOrReplaceTempView('data_attendent_18')

    data_casualty_18 = spark.sql(
        "SELECT arefno AS id,borough,casualtyclass,casualtysex,casualtyagebanded,casualtyseverity,modeoftravel FROM"
        " data_casualty_18").cache()

    split_class = f.split(data_casualty_18['casualtysex'], '^[^\s]*\s')
    data_casualty_18 = data_casualty_18.withColumn('csex', split_class.getItem(1))

    split_csex = f.split(data_casualty_18['casualtyclass'], '^[^\s]*\s')
    data_casualty_18 = data_casualty_18.withColumn('class', split_csex.getItem(1))

    split_cseverity = f.split(data_casualty_18['casualtyseverity'], '^[^\s]*\s')
    data_casualty_18 = data_casualty_18.withColumn('cseverity', split_cseverity.getItem(1))

    split_travel = f.split(data_casualty_18['modeoftravel'], '^[^\s]*\s')
    data_casualty_18 = data_casualty_18.withColumn('travel', split_travel.getItem(1))

    data_casualty_18.createOrReplaceTempView('data_casualty_18')

    table_sexage_18 = spark.sql("SELECT casualtyagebanded,csex,borough,cseverity from data_casualty_18 where cseverity IN \
                             ('Serious','Fatal') AND casualtyagebanded != 'Unknown'")

    table_sexage_18.createOrReplaceTempView('table_sexage_18')

    report_borough_18 = spark.sql(
        "SELECT casualtyagebanded AS age,csex,count(csex) AS count,borough FROM table_sexage_18 \
         GROUP BY casualtyagebanded,csex,borough,cseverity ORDER BY casualtyagebanded")

    report_borough_18.createOrReplaceTempView('report_borough_18')

    table_borough_18 = spark.sql(
        "SELECT A.id,A.borough,A.severity AS accidentseverity,A.numofcasualties,C.travel AS modeoftravel FROM"
        " data_attendent_18 as A INNER JOIN data_casualty_18 as C on A.id = C.id")
    table_borough_18.createOrReplaceTempView('table_borough_18')
    """""""""""""""""""""""""""""""""""""""""""""""
    18 ENDS HERE!!!!!
    """""""""""""""""""""""""""""""""""""""""""""""

    """""""""""""""""""""""""""""""""""""""""""""""
    WRITING TABLES TO POSTGRESQL
    """""""""""""""""""""""""""""""""""""""""""""""

    data_attendent_18.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "attendent_18") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .save()

    table_borough_18.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "borough_18") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .save()

    table_sexage_18.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "sexage_18") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .save()


    report_borough_18.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "report_borough_18") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .save()

    data_attendent_17.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "attendent_17") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .save()

    table_borough_17.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "borough_17") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .save()

    table_sexage_17.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "sexage_17") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .save()


    report_borough_17.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "report_borough_17") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .save()

    data_attendent_16.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "attendent_16") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .save()

    table_borough_16.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "borough_16") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .save()

    table_sexage_16.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "sexage_16") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .save()


    report_borough_16.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "report_borough_16") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .save()

    data_attendent_15.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "attendent_15") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .save()

    table_borough_15.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "borough_15") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .save()

    table_sexage_15.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "sexage_15") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .save()


    report_borough_15.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "report_borough_15") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .save()

    data_attendent_14.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "attendent_14") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .save()

    table_borough_14.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "borough_14") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .save()

    table_sexage_14.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "sexage_14") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .save()


    report_borough_14.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "report_borough_14") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .save()

    # Create table borough_summary first
    sum_list = [
        "INSERT into borough_summary SELECT SUM(numofcasualties) as num,modeoftravel,borough,2014 as year FROM borough_14 where accidentseverity IN ('Serious','Fatal')  GROUP BY borough,modeoftravel",
        "INSERT into borough_summary SELECT SUM(numofcasualties) as num,modeoftravel,borough,2015 as year FROM borough_15 where accidentseverity IN ('Serious','Fatal')  GROUP BY borough,modeoftravel",
        "INSERT into borough_summary SELECT SUM(numofcasualties) as num,modeoftravel,borough,2016 as year FROM borough_16 where accidentseverity IN ('Serious','Fatal')  GROUP BY borough,modeoftravel",
        "INSERT into borough_summary SELECT SUM(numofcasualties) as num,modeoftravel,borough,2017 as year FROM borough_17 where accidentseverity IN ('Serious','Fatal')  GROUP BY borough,modeoftravel",
        "INSERT into borough_summary SELECT SUM(numofcasualties) as num,modeoftravel,borough,2018 as year FROM borough_18 where accidentseverity IN ('Serious','Fatal')  GROUP BY borough,modeoftravel",
        ]

    day_count = [
        "INSERT into day_count SELECT daynum,count(daynum) as count,2014 as year FROM attendent_14 GROUP BY daynum",
        "INSERT into day_count SELECT daynum,count(daynum) as count,2015 as year FROM attendent_15 GROUP BY daynum",
        "INSERT into day_count SELECT daynum,count(daynum) as count,2016 as year FROM attendent_16 GROUP BY daynum",
        "INSERT into day_count SELECT daynum,count(daynum) as count,2017 as year FROM attendent_17 GROUP BY daynum",
        "INSERT into day_count SELECT daynum,count(daynum) as count,2018 as year FROM attendent_18 GROUP BY daynum",
    ]

    weather_count = [
        "INSERT into weather_count SELECT weathersplit as weather,count(weathersplit) as count,2014 as year FROM attendent_14 GROUP BY weathersplit",
        "INSERT into weather_count SELECT weathersplit as weather,count(weathersplit) as count,2015 as year FROM attendent_15 GROUP BY weathersplit",
        "INSERT into weather_count SELECT weathersplit as weather,count(weathersplit) as count,2016 as year FROM attendent_16 GROUP BY weathersplit",
        "INSERT into weather_count SELECT weathersplit as weather,count(weathersplit) as count,2017 as year FROM attendent_17 GROUP BY weathersplit",
        "INSERT into weather_count SELECT weathersplit as weather,count(weathersplit) as count,2018 as year FROM attendent_18 GROUP BY weathersplit",
    ]

    time_count = [
        "INSERT into time_count SELECT timebanded as timeband,count(timebanded) as count,2014 as year FROM attendent_14 GROUP BY timebanded",
        "INSERT into time_count SELECT timebanded as timeband,count(timebanded) as count,2015 as year FROM attendent_15 GROUP BY timebanded",
        "INSERT into time_count SELECT timebanded as timeband,count(timebanded) as count,2016 as year FROM attendent_16 GROUP BY timebanded",
        "INSERT into time_count SELECT timebanded as timeband,count(timebanded) as count,2017 as year FROM attendent_17 GROUP BY timebanded",
        "INSERT into time_count SELECT timebanded as timeband,count(timebanded) as count,2018 as year FROM attendent_18 GROUP BY timebanded",
    ]

    borough_count = [
        "INSERT into borough_count SELECT borough,count(borough) as count,2014 as year FROM attendent_14 GROUP BY borough",
        "INSERT into borough_count SELECT borough,count(borough) as count,2015 as year FROM attendent_15 GROUP BY borough",
        "INSERT into borough_count SELECT borough,count(borough) as count,2016 as year FROM attendent_16 GROUP BY borough",
        "INSERT into borough_count SELECT borough,count(borough) as count,2017 as year FROM attendent_17 GROUP BY borough",
        "INSERT into borough_count SELECT borough,count(borough) as count,2018 as year FROM attendent_18 GROUP BY borough",
    ]

    connection = psycopg2.connect(user="anuj_db",
                                  password="anuj_db",
                                  host="localhost",
                                  port="5432",
                                  database="anuj_db")

    if (connection):
        cursor = connection.cursor()
        print("CONNECTION OPENED")
        print("PostgreSQL connection is opened")
        for l in sum_list:
            cursor.execute(l)
            connection.commit()
            print("SUMMARY INSERTED")
        for l in day_count:
            cursor.execute(l)
            connection.commit()
            print("DAYCOUNT INSERTED")
        for l in time_count:
            cursor.execute(l)
            connection.commit()
            print("TIMECOUNT INSERTED")
        for l in weather_count:
            cursor.execute(l)
            connection.commit()
            print("WEATHERCOUNT INSERTED")
        for l in borough_count:
            cursor.execute(l)
            connection.commit()
            print("BOROUGHCOUNT INSERTED")
    cursor.close()
    connection.close()
    print("PostgreSQL connection is closed")

    year_list = ['report_borough_14', 'report_borough_15', 'report_borough_16', 'report_borough_17',
                 'report_borough_18']
    for i in year_list:
        connection = psycopg2.connect(user="anuj_db",
                                      password="anuj_db",
                                      host="localhost",
                                      port="5432",
                                      database="anuj_db")

        query_list = []
        query_list = [
            "Insert into  " + i + " SELECT '0-15','Male',0,'RICHMOND-UPON-THAMES' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Male' and borough='RICHMOND-UPON-THAMES')"
            ,
            "Insert into  " + i + " SELECT '0-15','Male',0,'CROYDON' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Male' and borough='CROYDON')"
            ,
            "Insert into  " + i + " SELECT '0-15','Male',0,'TOWER HAMLETS' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Male' and borough='TOWER HAMLETS')"
            ,
            "Insert into  " + i + " SELECT '0-15','Male',0,'GREENWICH' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Male' and borough='GREENWICH')"
            ,
            "Insert into  " + i + " SELECT '0-15','Male',0,'CAMDEN' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Male' and borough='CAMDEN')"
            ,
            "Insert into  " + i + " SELECT '0-15','Male',0,'HAMMERSMITH & FULHAM' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Male' and borough='HAMMERSMITH & FULHAM')"
            ,
            "Insert into  " + i + " SELECT '0-15','Male',0,'ENFIELD' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Male' and borough='ENFIELD')"
            ,
            "Insert into  " + i + " SELECT '0-15','Male',0,'HOUNSLOW' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Male' and borough='HOUNSLOW')"
            ,
            "Insert into  " + i + " SELECT '0-15','Male',0,'BEXLEY' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Male' and borough='BEXLEY')"
            ,
            "Insert into  " + i + " SELECT '0-15','Male',0,'LEWISHAM' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Male' and borough='LEWISHAM')"
            ,
            "Insert into  " + i + " SELECT '0-15','Male',0,'REDBRIDGE' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Male' and borough='REDBRIDGE')"
            ,
            "Insert into  " + i + " SELECT '0-15','Male',0,'CITY OF LONDON' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Male' and borough='CITY OF LONDON')"
            ,
            "Insert into  " + i + " SELECT '0-15','Male',0,'LAMBETH' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Male' and borough='LAMBETH')"
            ,
            "Insert into  " + i + " SELECT '0-15','Male',0,'HARROW' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Male' and borough='HARROW')"
            ,
            "Insert into  " + i + " SELECT '0-15','Male',0,'WESTMINSTER' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Male' and borough='WESTMINSTER')"
            ,
            "Insert into  " + i + " SELECT '0-15','Male',0,'KINGSTON-UPON-THAMES' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Male' and borough='KINGSTON-UPON-THAMES')"
            ,
            "Insert into  " + i + " SELECT '0-15','Male',0,'BROMLEY' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Male' and borough='BROMLEY')"
            ,
            "Insert into  " + i + " SELECT '0-15','Male',0,'KENSINGTON & CHELSEA' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Male' and borough='KENSINGTON & CHELSEA')"
            ,
            "Insert into  " + i + " SELECT '0-15','Male',0,'SOUTHWARK' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Male' and borough='SOUTHWARK')"
            ,
            "Insert into  " + i + " SELECT '0-15','Male',0,'HARINGEY' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Male' and borough='HARINGEY')"
            ,
            "Insert into  " + i + " SELECT '0-15','Male',0,'WALTHAM FOREST' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Male' and borough='WALTHAM FOREST')"
            ,
            "Insert into  " + i + " SELECT '0-15','Male',0,'HILLINGDON' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Male' and borough='HILLINGDON')"
            ,
            "Insert into  " + i + " SELECT '0-15','Male',0,'SUTTON' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Male' and borough='SUTTON')"
            ,
            "Insert into  " + i + " SELECT '0-15','Male',0,'HAVERING' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Male' and borough='HAVERING')"
            ,
            "Insert into  " + i + " SELECT '0-15','Male',0,'MERTON' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Male' and borough='MERTON')"
            ,
            "Insert into  " + i + " SELECT '0-15','Male',0,'BRENT' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Male' and borough='BRENT')"
            ,
            "Insert into  " + i + " SELECT '0-15','Male',0,'WANDSWORTH' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Male' and borough='WANDSWORTH')"
            ,
            "Insert into  " + i + " SELECT '0-15','Male',0,'NEWHAM' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Male' and borough='NEWHAM')"
            ,
            "Insert into  " + i + " SELECT '0-15','Male',0,'HACKNEY' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Male' and borough='HACKNEY')"
            ,
            "Insert into  " + i + " SELECT '0-15','Male',0,'ISLINGTON' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Male' and borough='ISLINGTON')"
            ,
            "Insert into  " + i + " SELECT '0-15','Male',0,'BARNET' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Male' and borough='BARNET')"
            ,
            "Insert into  " + i + " SELECT '0-15','Male',0,'BARKING & DAGENHAM' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Male' and borough='BARKING & DAGENHAM')"
            ,
            "Insert into  " + i + " SELECT '0-15','Male',0,'EALING' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Male' and borough='EALING')",
            "Insert into  " + i + " SELECT '16-24','Male',0,'RICHMOND-UPON-THAMES' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Male' and borough='RICHMOND-UPON-THAMES')"
            ,
            "Insert into  " + i + " SELECT '16-24','Male',0,'CROYDON' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Male' and borough='CROYDON')"
            ,
            "Insert into  " + i + " SELECT '16-24','Male',0,'TOWER HAMLETS' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Male' and borough='TOWER HAMLETS')"
            ,
            "Insert into  " + i + " SELECT '16-24','Male',0,'GREENWICH' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Male' and borough='GREENWICH')"
            ,
            "Insert into  " + i + " SELECT '16-24','Male',0,'CAMDEN' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Male' and borough='CAMDEN')"
            ,
            "Insert into  " + i + " SELECT '16-24','Male',0,'HAMMERSMITH & FULHAM' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Male' and borough='HAMMERSMITH & FULHAM')"
            ,
            "Insert into  " + i + " SELECT '16-24','Male',0,'ENFIELD' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Male' and borough='ENFIELD')"
            ,
            "Insert into  " + i + " SELECT '16-24','Male',0,'HOUNSLOW' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Male' and borough='HOUNSLOW')"
            ,
            "Insert into  " + i + " SELECT '16-24','Male',0,'BEXLEY' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Male' and borough='BEXLEY')"
            ,
            "Insert into  " + i + " SELECT '16-24','Male',0,'LEWISHAM' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Male' and borough='LEWISHAM')"
            ,
            "Insert into  " + i + " SELECT '16-24','Male',0,'REDBRIDGE' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Male' and borough='REDBRIDGE')"
            ,
            "Insert into  " + i + " SELECT '16-24','Male',0,'CITY OF LONDON' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Male' and borough='CITY OF LONDON')"
            ,
            "Insert into  " + i + " SELECT '16-24','Male',0,'LAMBETH' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Male' and borough='LAMBETH')"
            ,
            "Insert into  " + i + " SELECT '16-24','Male',0,'HARROW' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Male' and borough='HARROW')"
            ,
            "Insert into  " + i + " SELECT '16-24','Male',0,'WESTMINSTER' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Male' and borough='WESTMINSTER')"
            ,
            "Insert into  " + i + " SELECT '16-24','Male',0,'KINGSTON-UPON-THAMES' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Male' and borough='KINGSTON-UPON-THAMES')"
            ,
            "Insert into  " + i + " SELECT '16-24','Male',0,'BROMLEY' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Male' and borough='BROMLEY')"
            ,
            "Insert into  " + i + " SELECT '16-24','Male',0,'KENSINGTON & CHELSEA' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Male' and borough='KENSINGTON & CHELSEA')"
            ,
            "Insert into  " + i + " SELECT '16-24','Male',0,'SOUTHWARK' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Male' and borough='SOUTHWARK')"
            ,
            "Insert into  " + i + " SELECT '16-24','Male',0,'HARINGEY' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Male' and borough='HARINGEY')"
            ,
            "Insert into  " + i + " SELECT '16-24','Male',0,'WALTHAM FOREST' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Male' and borough='WALTHAM FOREST')"
            ,
            "Insert into  " + i + " SELECT '16-24','Male',0,'HILLINGDON' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Male' and borough='HILLINGDON')"
            ,
            "Insert into  " + i + " SELECT '16-24','Male',0,'SUTTON' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Male' and borough='SUTTON')"
            ,
            "Insert into  " + i + " SELECT '16-24','Male',0,'HAVERING' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Male' and borough='HAVERING')"
            ,
            "Insert into  " + i + " SELECT '16-24','Male',0,'MERTON' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Male' and borough='MERTON')"
            ,
            "Insert into  " + i + " SELECT '16-24','Male',0,'BRENT' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Male' and borough='BRENT')"
            ,
            "Insert into  " + i + " SELECT '16-24','Male',0,'WANDSWORTH' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Male' and borough='WANDSWORTH')"
            ,
            "Insert into  " + i + " SELECT '16-24','Male',0,'NEWHAM' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Male' and borough='NEWHAM')"
            ,
            "Insert into  " + i + " SELECT '16-24','Male',0,'HACKNEY' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Male' and borough='HACKNEY')"
            ,
            "Insert into  " + i + " SELECT '16-24','Male',0,'ISLINGTON' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Male' and borough='ISLINGTON')"
            ,
            "Insert into  " + i + " SELECT '16-24','Male',0,'BARNET' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Male' and borough='BARNET')"
            ,
            "Insert into  " + i + " SELECT '16-24','Male',0,'BARKING & DAGENHAM' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Male' and borough='BARKING & DAGENHAM')"
            ,
            "Insert into  " + i + " SELECT '16-24','Male',0,'EALING' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Male' and borough='EALING')",
            "Insert into  " + i + " SELECT '25-59','Male',0,'RICHMOND-UPON-THAMES' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Male' and borough='RICHMOND-UPON-THAMES')"
            ,
            "Insert into  " + i + " SELECT '25-59','Male',0,'CROYDON' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Male' and borough='CROYDON')"
            ,
            "Insert into  " + i + " SELECT '25-59','Male',0,'TOWER HAMLETS' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Male' and borough='TOWER HAMLETS')"
            ,
            "Insert into  " + i + " SELECT '25-59','Male',0,'GREENWICH' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Male' and borough='GREENWICH')"
            ,
            "Insert into  " + i + " SELECT '25-59','Male',0,'CAMDEN' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Male' and borough='CAMDEN')"
            ,
            "Insert into  " + i + " SELECT '25-59','Male',0,'HAMMERSMITH & FULHAM' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Male' and borough='HAMMERSMITH & FULHAM')"
            ,
            "Insert into  " + i + " SELECT '25-59','Male',0,'ENFIELD' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Male' and borough='ENFIELD')"
            ,
            "Insert into  " + i + " SELECT '25-59','Male',0,'HOUNSLOW' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Male' and borough='HOUNSLOW')"
            ,
            "Insert into  " + i + " SELECT '25-59','Male',0,'BEXLEY' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Male' and borough='BEXLEY')"
            ,
            "Insert into  " + i + " SELECT '25-59','Male',0,'LEWISHAM' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Male' and borough='LEWISHAM')"
            ,
            "Insert into  " + i + " SELECT '25-59','Male',0,'REDBRIDGE' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Male' and borough='REDBRIDGE')"
            ,
            "Insert into  " + i + " SELECT '25-59','Male',0,'CITY OF LONDON' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Male' and borough='CITY OF LONDON')"
            ,
            "Insert into  " + i + " SELECT '25-59','Male',0,'LAMBETH' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Male' and borough='LAMBETH')"
            ,
            "Insert into  " + i + " SELECT '25-59','Male',0,'HARROW' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Male' and borough='HARROW')"
            ,
            "Insert into  " + i + " SELECT '25-59','Male',0,'WESTMINSTER' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Male' and borough='WESTMINSTER')"
            ,
            "Insert into  " + i + " SELECT '25-59','Male',0,'KINGSTON-UPON-THAMES' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Male' and borough='KINGSTON-UPON-THAMES')"
            ,
            "Insert into  " + i + " SELECT '25-59','Male',0,'BROMLEY' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Male' and borough='BROMLEY')"
            ,
            "Insert into  " + i + " SELECT '25-59','Male',0,'KENSINGTON & CHELSEA' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Male' and borough='KENSINGTON & CHELSEA')"
            ,
            "Insert into  " + i + " SELECT '25-59','Male',0,'SOUTHWARK' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Male' and borough='SOUTHWARK')"
            ,
            "Insert into  " + i + " SELECT '25-59','Male',0,'HARINGEY' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Male' and borough='HARINGEY')"
            ,
            "Insert into  " + i + " SELECT '25-59','Male',0,'WALTHAM FOREST' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Male' and borough='WALTHAM FOREST')"
            ,
            "Insert into  " + i + " SELECT '25-59','Male',0,'HILLINGDON' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Male' and borough='HILLINGDON')"
            ,
            "Insert into  " + i + " SELECT '25-59','Male',0,'SUTTON' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Male' and borough='SUTTON')"
            ,
            "Insert into  " + i + " SELECT '25-59','Male',0,'HAVERING' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Male' and borough='HAVERING')"
            ,
            "Insert into  " + i + " SELECT '25-59','Male',0,'MERTON' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Male' and borough='MERTON')"
            ,
            "Insert into  " + i + " SELECT '25-59','Male',0,'BRENT' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Male' and borough='BRENT')"
            ,
            "Insert into  " + i + " SELECT '25-59','Male',0,'WANDSWORTH' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Male' and borough='WANDSWORTH')"
            ,
            "Insert into  " + i + " SELECT '25-59','Male',0,'NEWHAM' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Male' and borough='NEWHAM')"
            ,
            "Insert into  " + i + " SELECT '25-59','Male',0,'HACKNEY' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Male' and borough='HACKNEY')"
            ,
            "Insert into  " + i + " SELECT '25-59','Male',0,'ISLINGTON' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Male' and borough='ISLINGTON')"
            ,
            "Insert into  " + i + " SELECT '25-59','Male',0,'BARNET' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Male' and borough='BARNET')"
            ,
            "Insert into  " + i + " SELECT '25-59','Male',0,'BARKING & DAGENHAM' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Male' and borough='BARKING & DAGENHAM')"
            ,
            "Insert into  " + i + " SELECT '25-59','Male',0,'EALING' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Male' and borough='EALING')",
            "Insert into  " + i + " SELECT '60+','Male',0,'RICHMOND-UPON-THAMES' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Male' and borough='RICHMOND-UPON-THAMES')"
            ,
            "Insert into  " + i + " SELECT '60+','Male',0,'CROYDON' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Male' and borough='CROYDON')"
            ,
            "Insert into  " + i + " SELECT '60+','Male',0,'TOWER HAMLETS' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Male' and borough='TOWER HAMLETS')"
            ,
            "Insert into  " + i + " SELECT '60+','Male',0,'GREENWICH' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Male' and borough='GREENWICH')"
            ,
            "Insert into  " + i + " SELECT '60+','Male',0,'CAMDEN' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Male' and borough='CAMDEN')"
            ,
            "Insert into  " + i + " SELECT '60+','Male',0,'HAMMERSMITH & FULHAM' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Male' and borough='HAMMERSMITH & FULHAM')"
            ,
            "Insert into  " + i + " SELECT '60+','Male',0,'ENFIELD' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Male' and borough='ENFIELD')"
            ,
            "Insert into  " + i + " SELECT '60+','Male',0,'HOUNSLOW' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Male' and borough='HOUNSLOW')"
            ,
            "Insert into  " + i + " SELECT '60+','Male',0,'BEXLEY' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Male' and borough='BEXLEY')"
            ,
            "Insert into  " + i + " SELECT '60+','Male',0,'LEWISHAM' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Male' and borough='LEWISHAM')"
            ,
            "Insert into  " + i + " SELECT '60+','Male',0,'REDBRIDGE' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Male' and borough='REDBRIDGE')"
            ,
            "Insert into  " + i + " SELECT '60+','Male',0,'CITY OF LONDON' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Male' and borough='CITY OF LONDON')"
            ,
            "Insert into  " + i + " SELECT '60+','Male',0,'LAMBETH' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Male' and borough='LAMBETH')"
            ,
            "Insert into  " + i + " SELECT '60+','Male',0,'HARROW' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Male' and borough='HARROW')"
            ,
            "Insert into  " + i + " SELECT '60+','Male',0,'WESTMINSTER' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Male' and borough='WESTMINSTER')"
            ,
            "Insert into  " + i + " SELECT '60+','Male',0,'KINGSTON-UPON-THAMES' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Male' and borough='KINGSTON-UPON-THAMES')"
            ,
            "Insert into  " + i + " SELECT '60+','Male',0,'BROMLEY' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Male' and borough='BROMLEY')"
            ,
            "Insert into  " + i + " SELECT '60+','Male',0,'KENSINGTON & CHELSEA' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Male' and borough='KENSINGTON & CHELSEA')"
            ,
            "Insert into  " + i + " SELECT '60+','Male',0,'SOUTHWARK' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Male' and borough='SOUTHWARK')"
            ,
            "Insert into  " + i + " SELECT '60+','Male',0,'HARINGEY' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Male' and borough='HARINGEY')"
            ,
            "Insert into  " + i + " SELECT '60+','Male',0,'WALTHAM FOREST' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Male' and borough='WALTHAM FOREST')"
            ,
            "Insert into  " + i + " SELECT '60+','Male',0,'HILLINGDON' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Male' and borough='HILLINGDON')"
            ,
            "Insert into  " + i + " SELECT '60+','Male',0,'SUTTON' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Male' and borough='SUTTON')"
            ,
            "Insert into  " + i + " SELECT '60+','Male',0,'HAVERING' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Male' and borough='HAVERING')"
            ,
            "Insert into  " + i + " SELECT '60+','Male',0,'MERTON' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Male' and borough='MERTON')"
            ,
            "Insert into  " + i + " SELECT '60+','Male',0,'BRENT' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Male' and borough='BRENT')"
            ,
            "Insert into  " + i + " SELECT '60+','Male',0,'WANDSWORTH' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Male' and borough='WANDSWORTH')"
            ,
            "Insert into  " + i + " SELECT '60+','Male',0,'NEWHAM' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Male' and borough='NEWHAM')"
            ,
            "Insert into  " + i + " SELECT '60+','Male',0,'HACKNEY' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Male' and borough='HACKNEY')"
            ,
            "Insert into  " + i + " SELECT '60+','Male',0,'ISLINGTON' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Male' and borough='ISLINGTON')"
            ,
            "Insert into  " + i + " SELECT '60+','Male',0,'BARNET' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Male' and borough='BARNET')"
            ,
            "Insert into  " + i + " SELECT '60+','Male',0,'BARKING & DAGENHAM' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Male' and borough='BARKING & DAGENHAM')"
            ,
            "Insert into  " + i + " SELECT '60+','Male',0,'EALING' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Male' and borough='EALING')",
            "Insert into  " + i + " SELECT '0-15','Female',0,'RICHMOND-UPON-THAMES' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Female' and borough='RICHMOND-UPON-THAMES')"
            ,
            "Insert into  " + i + " SELECT '0-15','Female',0,'CROYDON' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Female' and borough='CROYDON')"
            ,
            "Insert into  " + i + " SELECT '0-15','Female',0,'TOWER HAMLETS' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Female' and borough='TOWER HAMLETS')"
            ,
            "Insert into  " + i + " SELECT '0-15','Female',0,'GREENWICH' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Female' and borough='GREENWICH')"
            ,
            "Insert into  " + i + " SELECT '0-15','Female',0,'CAMDEN' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Female' and borough='CAMDEN')"
            ,
            "Insert into  " + i + " SELECT '0-15','Female',0,'HAMMERSMITH & FULHAM' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Female' and borough='HAMMERSMITH & FULHAM')"
            ,
            "Insert into  " + i + " SELECT '0-15','Female',0,'ENFIELD' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Female' and borough='ENFIELD')"
            ,
            "Insert into  " + i + " SELECT '0-15','Female',0,'HOUNSLOW' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Female' and borough='HOUNSLOW')"
            ,
            "Insert into  " + i + " SELECT '0-15','Female',0,'BEXLEY' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Female' and borough='BEXLEY')"
            ,
            "Insert into  " + i + " SELECT '0-15','Female',0,'LEWISHAM' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Female' and borough='LEWISHAM')"
            ,
            "Insert into  " + i + " SELECT '0-15','Female',0,'REDBRIDGE' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Female' and borough='REDBRIDGE')"
            ,
            "Insert into  " + i + " SELECT '0-15','Female',0,'CITY OF LONDON' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Female' and borough='CITY OF LONDON')"
            ,
            "Insert into  " + i + " SELECT '0-15','Female',0,'LAMBETH' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Female' and borough='LAMBETH')"
            ,
            "Insert into  " + i + " SELECT '0-15','Female',0,'HARROW' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Female' and borough='HARROW')"
            ,
            "Insert into  " + i + " SELECT '0-15','Female',0,'WESTMINSTER' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Female' and borough='WESTMINSTER')"
            ,
            "Insert into  " + i + " SELECT '0-15','Female',0,'KINGSTON-UPON-THAMES' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Female' and borough='KINGSTON-UPON-THAMES')"
            ,
            "Insert into  " + i + " SELECT '0-15','Female',0,'BROMLEY' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Female' and borough='BROMLEY')"
            ,
            "Insert into  " + i + " SELECT '0-15','Female',0,'KENSINGTON & CHELSEA' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Female' and borough='KENSINGTON & CHELSEA')"
            ,
            "Insert into  " + i + " SELECT '0-15','Female',0,'SOUTHWARK' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Female' and borough='SOUTHWARK')"
            ,
            "Insert into  " + i + " SELECT '0-15','Female',0,'HARINGEY' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Female' and borough='HARINGEY')"
            ,
            "Insert into  " + i + " SELECT '0-15','Female',0,'WALTHAM FOREST' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Female' and borough='WALTHAM FOREST')"
            ,
            "Insert into  " + i + " SELECT '0-15','Female',0,'HILLINGDON' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Female' and borough='HILLINGDON')"
            ,
            "Insert into  " + i + " SELECT '0-15','Female',0,'SUTTON' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Female' and borough='SUTTON')"
            ,
            "Insert into  " + i + " SELECT '0-15','Female',0,'HAVERING' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Female' and borough='HAVERING')"
            ,
            "Insert into  " + i + " SELECT '0-15','Female',0,'MERTON' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Female' and borough='MERTON')"
            ,
            "Insert into  " + i + " SELECT '0-15','Female',0,'BRENT' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Female' and borough='BRENT')"
            ,
            "Insert into  " + i + " SELECT '0-15','Female',0,'WANDSWORTH' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Female' and borough='WANDSWORTH')"
            ,
            "Insert into  " + i + " SELECT '0-15','Female',0,'NEWHAM' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Female' and borough='NEWHAM')"
            ,
            "Insert into  " + i + " SELECT '0-15','Female',0,'HACKNEY' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Female' and borough='HACKNEY')"
            ,
            "Insert into  " + i + " SELECT '0-15','Female',0,'ISLINGTON' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Female' and borough='ISLINGTON')"
            ,
            "Insert into  " + i + " SELECT '0-15','Female',0,'BARNET' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Female' and borough='BARNET')"
            ,
            "Insert into  " + i + " SELECT '0-15','Female',0,'BARKING & DAGENHAM' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Female' and borough='BARKING & DAGENHAM')"
            ,
            "Insert into  " + i + " SELECT '0-15','Female',0,'EALING' where Not exists (select * from  " + i + " where count>0 and age='0-15' and csex='Female' and borough='EALING')",
            "Insert into  " + i + " SELECT '16-24','Female',0,'RICHMOND-UPON-THAMES' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Female' and borough='RICHMOND-UPON-THAMES')"
            ,
            "Insert into  " + i + " SELECT '16-24','Female',0,'CROYDON' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Female' and borough='CROYDON')"
            ,
            "Insert into  " + i + " SELECT '16-24','Female',0,'TOWER HAMLETS' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Female' and borough='TOWER HAMLETS')"
            ,
            "Insert into  " + i + " SELECT '16-24','Female',0,'GREENWICH' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Female' and borough='GREENWICH')"
            ,
            "Insert into  " + i + " SELECT '16-24','Female',0,'CAMDEN' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Female' and borough='CAMDEN')"
            ,
            "Insert into  " + i + " SELECT '16-24','Female',0,'HAMMERSMITH & FULHAM' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Female' and borough='HAMMERSMITH & FULHAM')"
            ,
            "Insert into  " + i + " SELECT '16-24','Female',0,'ENFIELD' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Female' and borough='ENFIELD')"
            ,
            "Insert into  " + i + " SELECT '16-24','Female',0,'HOUNSLOW' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Female' and borough='HOUNSLOW')"
            ,
            "Insert into  " + i + " SELECT '16-24','Female',0,'BEXLEY' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Female' and borough='BEXLEY')"
            ,
            "Insert into  " + i + " SELECT '16-24','Female',0,'LEWISHAM' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Female' and borough='LEWISHAM')"
            ,
            "Insert into  " + i + " SELECT '16-24','Female',0,'REDBRIDGE' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Female' and borough='REDBRIDGE')"
            ,
            "Insert into  " + i + " SELECT '16-24','Female',0,'CITY OF LONDON' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Female' and borough='CITY OF LONDON')"
            ,
            "Insert into  " + i + " SELECT '16-24','Female',0,'LAMBETH' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Female' and borough='LAMBETH')"
            ,
            "Insert into  " + i + " SELECT '16-24','Female',0,'HARROW' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Female' and borough='HARROW')"
            ,
            "Insert into  " + i + " SELECT '16-24','Female',0,'WESTMINSTER' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Female' and borough='WESTMINSTER')"
            ,
            "Insert into  " + i + " SELECT '16-24','Female',0,'KINGSTON-UPON-THAMES' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Female' and borough='KINGSTON-UPON-THAMES')"
            ,
            "Insert into  " + i + " SELECT '16-24','Female',0,'BROMLEY' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Female' and borough='BROMLEY')"
            ,
            "Insert into  " + i + " SELECT '16-24','Female',0,'KENSINGTON & CHELSEA' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Female' and borough='KENSINGTON & CHELSEA')"
            ,
            "Insert into  " + i + " SELECT '16-24','Female',0,'SOUTHWARK' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Female' and borough='SOUTHWARK')"
            ,
            "Insert into  " + i + " SELECT '16-24','Female',0,'HARINGEY' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Female' and borough='HARINGEY')"
            ,
            "Insert into  " + i + " SELECT '16-24','Female',0,'WALTHAM FOREST' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Female' and borough='WALTHAM FOREST')"
            ,
            "Insert into  " + i + " SELECT '16-24','Female',0,'HILLINGDON' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Female' and borough='HILLINGDON')"
            ,
            "Insert into  " + i + " SELECT '16-24','Female',0,'SUTTON' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Female' and borough='SUTTON')"
            ,
            "Insert into  " + i + " SELECT '16-24','Female',0,'HAVERING' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Female' and borough='HAVERING')"
            ,
            "Insert into  " + i + " SELECT '16-24','Female',0,'MERTON' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Female' and borough='MERTON')"
            ,
            "Insert into  " + i + " SELECT '16-24','Female',0,'BRENT' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Female' and borough='BRENT')"
            ,
            "Insert into  " + i + " SELECT '16-24','Female',0,'WANDSWORTH' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Female' and borough='WANDSWORTH')"
            ,
            "Insert into  " + i + " SELECT '16-24','Female',0,'NEWHAM' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Female' and borough='NEWHAM')"
            ,
            "Insert into  " + i + " SELECT '16-24','Female',0,'HACKNEY' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Female' and borough='HACKNEY')"
            ,
            "Insert into  " + i + " SELECT '16-24','Female',0,'ISLINGTON' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Female' and borough='ISLINGTON')"
            ,
            "Insert into  " + i + " SELECT '16-24','Female',0,'BARNET' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Female' and borough='BARNET')"
            ,
            "Insert into  " + i + " SELECT '16-24','Female',0,'BARKING & DAGENHAM' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Female' and borough='BARKING & DAGENHAM')"
            ,
            "Insert into  " + i + " SELECT '16-24','Female',0,'EALING' where Not exists (select * from  " + i + " where count>0 and age='16-24' and csex='Female' and borough='EALING')",
            "Insert into  " + i + " SELECT '25-59','Female',0,'RICHMOND-UPON-THAMES' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Female' and borough='RICHMOND-UPON-THAMES')"
            ,
            "Insert into  " + i + " SELECT '25-59','Female',0,'CROYDON' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Female' and borough='CROYDON')"
            ,
            "Insert into  " + i + " SELECT '25-59','Female',0,'TOWER HAMLETS' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Female' and borough='TOWER HAMLETS')"
            ,
            "Insert into  " + i + " SELECT '25-59','Female',0,'GREENWICH' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Female' and borough='GREENWICH')"
            ,
            "Insert into  " + i + " SELECT '25-59','Female',0,'CAMDEN' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Female' and borough='CAMDEN')"
            ,
            "Insert into  " + i + " SELECT '25-59','Female',0,'HAMMERSMITH & FULHAM' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Female' and borough='HAMMERSMITH & FULHAM')"
            ,
            "Insert into  " + i + " SELECT '25-59','Female',0,'ENFIELD' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Female' and borough='ENFIELD')"
            ,
            "Insert into  " + i + " SELECT '25-59','Female',0,'HOUNSLOW' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Female' and borough='HOUNSLOW')"
            ,
            "Insert into  " + i + " SELECT '25-59','Female',0,'BEXLEY' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Female' and borough='BEXLEY')"
            ,
            "Insert into  " + i + " SELECT '25-59','Female',0,'LEWISHAM' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Female' and borough='LEWISHAM')"
            ,
            "Insert into  " + i + " SELECT '25-59','Female',0,'REDBRIDGE' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Female' and borough='REDBRIDGE')"
            ,
            "Insert into  " + i + " SELECT '25-59','Female',0,'CITY OF LONDON' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Female' and borough='CITY OF LONDON')"
            ,
            "Insert into  " + i + " SELECT '25-59','Female',0,'LAMBETH' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Female' and borough='LAMBETH')"
            ,
            "Insert into  " + i + " SELECT '25-59','Female',0,'HARROW' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Female' and borough='HARROW')"
            ,
            "Insert into  " + i + " SELECT '25-59','Female',0,'WESTMINSTER' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Female' and borough='WESTMINSTER')"
            ,
            "Insert into  " + i + " SELECT '25-59','Female',0,'KINGSTON-UPON-THAMES' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Female' and borough='KINGSTON-UPON-THAMES')"
            ,
            "Insert into  " + i + " SELECT '25-59','Female',0,'BROMLEY' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Female' and borough='BROMLEY')"
            ,
            "Insert into  " + i + " SELECT '25-59','Female',0,'KENSINGTON & CHELSEA' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Female' and borough='KENSINGTON & CHELSEA')"
            ,
            "Insert into  " + i + " SELECT '25-59','Female',0,'SOUTHWARK' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Female' and borough='SOUTHWARK')"
            ,
            "Insert into  " + i + " SELECT '25-59','Female',0,'HARINGEY' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Female' and borough='HARINGEY')"
            ,
            "Insert into  " + i + " SELECT '25-59','Female',0,'WALTHAM FOREST' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Female' and borough='WALTHAM FOREST')"
            ,
            "Insert into  " + i + " SELECT '25-59','Female',0,'HILLINGDON' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Female' and borough='HILLINGDON')"
            ,
            "Insert into  " + i + " SELECT '25-59','Female',0,'SUTTON' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Female' and borough='SUTTON')"
            ,
            "Insert into  " + i + " SELECT '25-59','Female',0,'HAVERING' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Female' and borough='HAVERING')"
            ,
            "Insert into  " + i + " SELECT '25-59','Female',0,'MERTON' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Female' and borough='MERTON')"
            ,
            "Insert into  " + i + " SELECT '25-59','Female',0,'BRENT' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Female' and borough='BRENT')"
            ,
            "Insert into  " + i + " SELECT '25-59','Female',0,'WANDSWORTH' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Female' and borough='WANDSWORTH')"
            ,
            "Insert into  " + i + " SELECT '25-59','Female',0,'NEWHAM' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Female' and borough='NEWHAM')"
            ,
            "Insert into  " + i + " SELECT '25-59','Female',0,'HACKNEY' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Female' and borough='HACKNEY')"
            ,
            "Insert into  " + i + " SELECT '25-59','Female',0,'ISLINGTON' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Female' and borough='ISLINGTON')"
            ,
            "Insert into  " + i + " SELECT '25-59','Female',0,'BARNET' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Female' and borough='BARNET')"
            ,
            "Insert into  " + i + " SELECT '25-59','Female',0,'BARKING & DAGENHAM' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Female' and borough='BARKING & DAGENHAM')"
            ,
            "Insert into  " + i + " SELECT '25-59','Female',0,'EALING' where Not exists (select * from  " + i + " where count>0 and age='25-59' and csex='Female' and borough='EALING')",
            "Insert into  " + i + " SELECT '60+','Female',0,'RICHMOND-UPON-THAMES' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Female' and borough='RICHMOND-UPON-THAMES')"
            ,
            "Insert into  " + i + " SELECT '60+','Female',0,'CROYDON' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Female' and borough='CROYDON')"
            ,
            "Insert into  " + i + " SELECT '60+','Female',0,'TOWER HAMLETS' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Female' and borough='TOWER HAMLETS')"
            ,
            "Insert into  " + i + " SELECT '60+','Female',0,'GREENWICH' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Female' and borough='GREENWICH')"
            ,
            "Insert into  " + i + " SELECT '60+','Female',0,'CAMDEN' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Female' and borough='CAMDEN')"
            ,
            "Insert into  " + i + " SELECT '60+','Female',0,'HAMMERSMITH & FULHAM' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Female' and borough='HAMMERSMITH & FULHAM')"
            ,
            "Insert into  " + i + " SELECT '60+','Female',0,'ENFIELD' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Female' and borough='ENFIELD')"
            ,
            "Insert into  " + i + " SELECT '60+','Female',0,'HOUNSLOW' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Female' and borough='HOUNSLOW')"
            ,
            "Insert into  " + i + " SELECT '60+','Female',0,'BEXLEY' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Female' and borough='BEXLEY')"
            ,
            "Insert into  " + i + " SELECT '60+','Female',0,'LEWISHAM' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Female' and borough='LEWISHAM')"
            ,
            "Insert into  " + i + " SELECT '60+','Female',0,'REDBRIDGE' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Female' and borough='REDBRIDGE')"
            ,
            "Insert into  " + i + " SELECT '60+','Female',0,'CITY OF LONDON' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Female' and borough='CITY OF LONDON')"
            ,
            "Insert into  " + i + " SELECT '60+','Female',0,'LAMBETH' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Female' and borough='LAMBETH')"
            ,
            "Insert into  " + i + " SELECT '60+','Female',0,'HARROW' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Female' and borough='HARROW')"
            ,
            "Insert into  " + i + " SELECT '60+','Female',0,'WESTMINSTER' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Female' and borough='WESTMINSTER')"
            ,
            "Insert into  " + i + " SELECT '60+','Female',0,'KINGSTON-UPON-THAMES' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Female' and borough='KINGSTON-UPON-THAMES')"
            ,
            "Insert into  " + i + " SELECT '60+','Female',0,'BROMLEY' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Female' and borough='BROMLEY')"
            ,
            "Insert into  " + i + " SELECT '60+','Female',0,'KENSINGTON & CHELSEA' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Female' and borough='KENSINGTON & CHELSEA')"
            ,
            "Insert into  " + i + " SELECT '60+','Female',0,'SOUTHWARK' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Female' and borough='SOUTHWARK')"
            ,
            "Insert into  " + i + " SELECT '60+','Female',0,'HARINGEY' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Female' and borough='HARINGEY')"
            ,
            "Insert into  " + i + " SELECT '60+','Female',0,'WALTHAM FOREST' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Female' and borough='WALTHAM FOREST')"
            ,
            "Insert into  " + i + " SELECT '60+','Female',0,'HILLINGDON' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Female' and borough='HILLINGDON')"
            ,
            "Insert into  " + i + " SELECT '60+','Female',0,'SUTTON' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Female' and borough='SUTTON')"
            ,
            "Insert into  " + i + " SELECT '60+','Female',0,'HAVERING' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Female' and borough='HAVERING')"
            ,
            "Insert into  " + i + " SELECT '60+','Female',0,'MERTON' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Female' and borough='MERTON')"
            ,
            "Insert into  " + i + " SELECT '60+','Female',0,'BRENT' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Female' and borough='BRENT')"
            ,
            "Insert into  " + i + " SELECT '60+','Female',0,'WANDSWORTH' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Female' and borough='WANDSWORTH')"
            ,
            "Insert into  " + i + " SELECT '60+','Female',0,'NEWHAM' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Female' and borough='NEWHAM')"
            ,
            "Insert into  " + i + " SELECT '60+','Female',0,'HACKNEY' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Female' and borough='HACKNEY')"
            ,
            "Insert into  " + i + " SELECT '60+','Female',0,'ISLINGTON' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Female' and borough='ISLINGTON')"
            ,
            "Insert into  " + i + " SELECT '60+','Female',0,'BARNET' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Female' and borough='BARNET')"
            ,
            "Insert into  " + i + " SELECT '60+','Female',0,'BARKING & DAGENHAM' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Female' and borough='BARKING & DAGENHAM')"
            ,
            "Insert into  " + i + " SELECT '60+','Female',0,'EALING' where Not exists (select * from  " + i + " where count>0 and age='60+' and csex='Female' and borough='EALING')"

        ]

        if (connection):
            cursor = connection.cursor()
            print("CONNECTION OPENED")
            print("PostgreSQL connection is opened")
            for q in query_list:
                cursor.execute(q)
                connection.commit()
                print("DATA INSERTED")

        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")


if __name__ == '__main__':
    main()