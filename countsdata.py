import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, types

spark = SparkSession.builder.appName('Train Data Analysis').config('spark.driver.extraClassPath','postgresql-42.2.8.jar').getOrCreate()
assert spark.version >= '2.4'  # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

schema_counts = types.StructType([
    types.StructField('locationcode', types.IntegerType()),
    types.StructField('station', types.StringType()),
    types.StructField('borough', types.StringType()),
    types.StructField('note', types.StringType()),
    types.StructField('entryweekday', types.LongType()),
    types.StructField('entrysaturday', types.LongType()),
    types.StructField('entrysunday', types.LongType()),
    types.StructField('exitweekday', types.LongType()),
    types.StructField('exitsaturday', types.LongType()),
    types.StructField('exitsunday', types.LongType()),
    types.StructField('entryexitinmillion', types.DoubleType()),
])


def main():
    counts()

def counts():
    file_counts_17 = '/home/anuj/Desktop/732project/data/counts/2017entryexit.csv'
    data_counts_17 = spark.read.csv(file_counts_17, header='true', schema=schema_counts)
    data_counts_17.createOrReplaceTempView('data_counts_17')
    data_counts_17 = spark.sql("SELECT station,borough,entryweekday,entrysaturday,entrysunday,exitweekday,"
                               "exitsaturday,exitsunday,(((253*entryweekday) + (52*entrysaturday) + (59*entrysunday) "
                               "+ (253*exitweekday) + (52*exitsaturday) + (59*exitsunday))/1000000) AS "
                               "entryexitinmillion FROM data_counts_17")
    data_counts_17.createOrReplaceTempView('data_counts_17')
    netload_per_borough_17 = data_counts_17.groupBy('borough').agg({'entryexitinmillion': 'sum'})
    # netload_per_borough_17.show(20, False)


    entryexit_17 = data_counts_17.orderBy("entryexitinmillion", ascending=False).limit(10)

    # entryexit_17.show()

    file_counts_16 = '/home/anuj/Desktop/732project/data/counts/2016entryexit.csv'
    data_counts_16 = spark.read.csv(file_counts_16, header='true', schema=schema_counts)
    data_counts_16.createOrReplaceTempView('data_counts_16')
    data_counts_16 = spark.sql("SELECT station,borough,entryweekday,entrysaturday,entrysunday,exitweekday,"
                               "exitsaturday,exitsunday,(((253*entryweekday) + (52*entrysaturday) + (59*entrysunday) "
                               "+ (253*exitweekday) + (52*exitsaturday) + (59*exitsunday))/1000000) AS "
                               "entryexitinmillion FROM data_counts_16")
    data_counts_16.createOrReplaceTempView('data_counts_16')
    netload_per_borough_16 = data_counts_16.groupBy('borough').agg({'entryexitinmillion': 'sum'})
    # netload_per_borough_16.show(10, False)

    entryexit_16 = data_counts_16.orderBy("entryexitinmillion", ascending=False).limit(10)
    # entryexit_16.show()

    file_counts_15 = '/home/anuj/Desktop/732project/data/counts/2015entryexit.csv'
    data_counts_15 = spark.read.csv(file_counts_15, header='true', schema=schema_counts)
    data_counts_15.createOrReplaceTempView('data_counts_15')
    data_counts_15 = spark.sql("SELECT station,borough,entryweekday,entrysaturday,entrysunday,exitweekday,"
                               "exitsaturday,exitsunday,(((253*entryweekday) + (52*entrysaturday) + (59*entrysunday) "
                               "+ (253*exitweekday) + (52*exitsaturday) + (59*exitsunday))/1000000) AS "
                               "entryexitinmillion FROM data_counts_15")
    data_counts_15.createOrReplaceTempView('data_counts_15')
    netload_per_borough_15 = data_counts_15.groupBy('borough').agg({'entryexitinmillion': 'sum'})
    # netload_per_borough_15.show(10, False)

    entryexit_15 = data_counts_15.orderBy("entryexitinmillion", ascending=False).limit(10)
    # entryexit_15.show()

    file_counts_14 = '/home/anuj/Desktop/732project/data/counts/2014entryexit.csv'
    data_counts_14 = spark.read.csv(file_counts_14, header='true', schema=schema_counts)
    data_counts_14.createOrReplaceTempView('data_counts_14')
    data_counts_14 = spark.sql("SELECT station,borough,entryweekday,entrysaturday,entrysunday,exitweekday,"
                               "exitsaturday,exitsunday,(((253*entryweekday) + (52*entrysaturday) + (59*entrysunday) "
                               "+ (253*exitweekday) + (52*exitsaturday) + (59*exitsunday))/1000000) AS "
                               "entryexitinmillion FROM data_counts_14")
    data_counts_14.createOrReplaceTempView('data_counts_14')
    netload_per_borough_14 = data_counts_14.groupBy('borough').agg({'entryexitinmillion': 'sum'})
    # netload_per_borough_14.show(50, False)

    entryexit_14 = data_counts_14.orderBy("entryexitinmillion", ascending=False).limit(10)
    # entryexit_14.show()

    file_counts_13 = '/home/anuj/Desktop/732project/data/counts/2013entryexit.csv'
    data_counts_13 = spark.read.csv(file_counts_13, header='true', schema=schema_counts)
    data_counts_13.createOrReplaceTempView('data_counts_13')
    data_counts_13 = spark.sql("SELECT station,borough,entryweekday,entrysaturday,entrysunday,exitweekday,"
                               "exitsaturday,exitsunday,(((253*entryweekday) + (52*entrysaturday) + (59*entrysunday) "
                               "+ (253*exitweekday) + (52*exitsaturday) + (59*exitsunday))/1000000) AS "
                               "entryexitinmillion FROM data_counts_13")
    data_counts_13.createOrReplaceTempView('data_counts_13')
    netload_per_borough_13 = data_counts_13.groupBy('borough').agg({'entryexitinmillion': 'sum'})
    netload_per_borough_13 = netload_per_borough_13.na.drop(subset=["borough"])
    # netload_per_borough_13.show(50, False)

    entryexit_13 = data_counts_13.orderBy("entryexitinmillion", ascending=False).limit(10)
    # entryexit_13.show()

    netload_per_borough_17.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "netload_per_borough_17") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .save()
    entryexit_17.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "entryexit_17") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .save()

    netload_per_borough_16.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "netload_per_borough_16") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .save()
    entryexit_16.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "entryexit_16") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .save()

    netload_per_borough_15.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "netload_per_borough_15") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .save()
    entryexit_15.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "entryexit_15") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .save()

    netload_per_borough_14.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "netload_per_borough_14") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .save()
    entryexit_14.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "entryexit_14") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .save()

    netload_per_borough_13.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "netload_per_borough_13") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .save()
    entryexit_13.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "entryexit_13") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .save()


if __name__ == '__main__':
    main()