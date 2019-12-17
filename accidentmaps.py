import sys
import statistics
import folium
from folium.plugins import MarkerCluster

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession
from bng_to_latlon import OSGB36toWGS84

spark = SparkSession.builder.appName('Accident Analysis').getOrCreate()
assert spark.version >= '2.4'  # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext


def accidentmap():
    data_attendent_18 = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "attendent_18") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .load()

    data_attendent_17 = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "attendent_17") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .load()

    data_attendent_16 = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "attendent_16") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .load()

    data_attendent_15 = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "attendent_15") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .load()

    data_attendent_14 = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "attendent_14") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .load()

    """""""""""""""""""""""""""""""""""""""""""""""
    LATITUDE LONGITUDE CONVERSION 2018
    """""""""""""""""""""""""""""""""""""""""""""""

    fatal_df = data_attendent_18.filter(data_attendent_18.severitynum == 1)
    east_18 = fatal_df.select("easting").rdd.flatMap(lambda x: x).collect()
    north_18 = fatal_df.select("northing").rdd.flatMap(lambda x: x).collect()
    casualty_fatal = fatal_df.select("numofcasualties").rdd.flatMap(lambda x: x).collect()
    date_fatal = fatal_df.select("accidentdate").rdd.flatMap(lambda x: x).collect()
    day_fatal = fatal_df.select("day").rdd.flatMap(lambda x: x).collect()
    time_fatal = fatal_df.select("time_new").rdd.flatMap(lambda x: x).collect()
    loc_fatal = fatal_df.select("location").rdd.flatMap(lambda x: x).collect()
    borough_fatal = fatal_df.select("borough").rdd.flatMap(lambda x: x).collect()

    lat_list_fatal = []
    long_list_fatal = []
    for i in range(len(east_18)):
        lat, long = OSGB36toWGS84(east_18[i], north_18[i])
        lat_list_fatal.append(lat)
        long_list_fatal.append(long)

    serious_df = data_attendent_18.filter(data_attendent_18.severitynum == 2)
    east_18 = serious_df.select("easting").rdd.flatMap(lambda x: x).collect()
    north_18 = serious_df.select("northing").rdd.flatMap(lambda x: x).collect()
    casualty_serious = serious_df.select("numofcasualties").rdd.flatMap(lambda x: x).collect()
    date_serious = serious_df.select("accidentdate").rdd.flatMap(lambda x: x).collect()
    day_serious = serious_df.select("day").rdd.flatMap(lambda x: x).collect()
    time_serious = serious_df.select("time_new").rdd.flatMap(lambda x: x).collect()
    loc_serious = serious_df.select("location").rdd.flatMap(lambda x: x).collect()
    borough_serious = serious_df.select("borough").rdd.flatMap(lambda x: x).collect()

    lat_list_serious = []
    long_list_serious = []
    for i in range(len(east_18)):
        lat, long = OSGB36toWGS84(east_18[i], north_18[i])
        lat_list_serious.append(lat)
        long_list_serious.append(long)

    """""""""""""""""""""""""""""""""""""""""""""""
    CLUSTER MAP 2018
    """""""""""""""""""""""""""""""""""""""""""""""
    mean_lat = statistics.mean(lat_list_serious)
    mean_long = statistics.mean(long_list_serious)
    acc_map_18 = folium.Map(location=[mean_lat, mean_long], zoom_start=10)
    marker_cluster = MarkerCluster().add_to(acc_map_18)

    for k in range(len(lat_list_fatal)):
        location = lat_list_fatal[k], long_list_fatal[k]
        popup = '<b>FATAL COLLISION, ' + str(casualty_fatal[k]) + ' KILLED</b> <br> <br>On ' + date_fatal[k] + ', ' + \
                day_fatal[k] + ' at ' + time_fatal[k] + ', an accident occured at ' + loc_fatal[k] + ' in ' + \
                borough_fatal[k] + ' killing ' + str(casualty_fatal[k])
        folium.Marker(
            location=location,
            clustered_marker=True,
            popup=folium.Popup(popup, max_width=300, min_width=300),
            icon=folium.Icon(color='red')).add_to(marker_cluster)

    for k in range(len(lat_list_serious)):
        location = lat_list_serious[k], long_list_serious[k]
        popup = '<b>SERIOUS COLLISION, ' + str(casualty_serious[k]) + ' INJURED</b> <br> <br>On ' + date_serious[
            k] + ', ' + \
                day_serious[k] + ' at ' + time_serious[k] + ', an accident occured at ' + loc_serious[k] + ' in ' + \
                borough_serious[k] + ' severely injuring ' + str(casualty_serious[k])
        folium.Marker(
            location=location,
            clustered_marker=True,
            popup=folium.Popup(popup, max_width=300, min_width=300),
            icon=folium.Icon(color='blue')).add_to(marker_cluster)

    acc_map_18.save('assets/map_18.html')

    """""""""""""""""""""""""""""""""""""""""""""""
    LATITUDE LONGITUDE CONVERSION 2017
    """""""""""""""""""""""""""""""""""""""""""""""
    fatal_df = data_attendent_17.filter(data_attendent_17.severitynum == 1)
    east_17 = fatal_df.select("easting").rdd.flatMap(lambda x: x).collect()
    north_17 = fatal_df.select("northing").rdd.flatMap(lambda x: x).collect()
    casualty_fatal = fatal_df.select("numofcasualties").rdd.flatMap(lambda x: x).collect()
    date_fatal = fatal_df.select("accidentdate").rdd.flatMap(lambda x: x).collect()
    day_fatal = fatal_df.select("day").rdd.flatMap(lambda x: x).collect()
    time_fatal = fatal_df.select("time_new").rdd.flatMap(lambda x: x).collect()
    loc_fatal = fatal_df.select("location").rdd.flatMap(lambda x: x).collect()
    borough_fatal = fatal_df.select("borough").rdd.flatMap(lambda x: x).collect()

    lat_list_fatal = []
    long_list_fatal = []
    for i in range(len(east_17)):
        lat, long = OSGB36toWGS84(east_17[i], north_17[i])
        lat_list_fatal.append(lat)
        long_list_fatal.append(long)

    serious_df = data_attendent_17.filter(data_attendent_17.severitynum == 2)
    east_17 = serious_df.select("easting").rdd.flatMap(lambda x: x).collect()
    north_17 = serious_df.select("northing").rdd.flatMap(lambda x: x).collect()
    casualty_serious = serious_df.select("numofcasualties").rdd.flatMap(lambda x: x).collect()
    date_serious = serious_df.select("accidentdate").rdd.flatMap(lambda x: x).collect()
    day_serious = serious_df.select("day").rdd.flatMap(lambda x: x).collect()
    time_serious = serious_df.select("time_new").rdd.flatMap(lambda x: x).collect()
    loc_serious = serious_df.select("location").rdd.flatMap(lambda x: x).collect()
    borough_serious = serious_df.select("borough").rdd.flatMap(lambda x: x).collect()

    lat_list_serious = []
    long_list_serious = []
    for i in range(len(east_17)):
        lat, long = OSGB36toWGS84(east_17[i], north_17[i])
        lat_list_serious.append(lat)
        long_list_serious.append(long)

    """""""""""""""""""""""""""""""""""""""""""""""
    CLUSTER MAP 2017
    """""""""""""""""""""""""""""""""""""""""""""""
    mean_lat = statistics.mean(lat_list_serious)
    mean_long = statistics.mean(long_list_serious)
    acc_map_17 = folium.Map(location=[mean_lat, mean_long], zoom_start=10)
    marker_cluster = MarkerCluster().add_to(acc_map_17)

    for k in range(len(lat_list_fatal)):
        location = lat_list_fatal[k], long_list_fatal[k]
        popup = '<b>FATAL COLLISION, ' + str(casualty_fatal[k]) + ' KILLED</b> <br> <br>On ' + date_fatal[k] + ', ' + \
                day_fatal[k] + ' at ' + time_fatal[k] + ', an accident occured at ' + loc_fatal[k] + ' in ' + \
                borough_fatal[k] + ' killing ' + str(casualty_fatal[k])
        folium.Marker(
            location=location,
            clustered_marker=True,
            popup=folium.Popup(popup, max_width=300, min_width=300),
            icon=folium.Icon(color='red')).add_to(marker_cluster)

    for k in range(len(lat_list_serious)):
        location = lat_list_serious[k], long_list_serious[k]
        popup = '<b>SERIOUS COLLISION, ' + str(casualty_serious[k]) + ' INJURED</b> <br> <br>On ' + date_serious[
            k] + ', ' + \
                day_serious[k] + ' at ' + time_serious[k] + ', an accident occured at ' + loc_serious[k] + ' in ' + \
                borough_serious[k] + ' severely injuring ' + str(casualty_serious[k])
        folium.Marker(
            location=location,
            clustered_marker=True,
            popup=folium.Popup(popup, max_width=300, min_width=300),
            icon=folium.Icon(color='blue')).add_to(marker_cluster)

    acc_map_17.save('assets/map_17.html')

    """""""""""""""""""""""""""""""""""""""""""""""
    LATITUDE LONGITUDE CONVERSION 2016
    """""""""""""""""""""""""""""""""""""""""""""""
    fatal_df = data_attendent_16.filter(data_attendent_16.severitynum == 1)
    east_16 = fatal_df.select("easting").rdd.flatMap(lambda x: x).collect()
    north_16 = fatal_df.select("northing").rdd.flatMap(lambda x: x).collect()
    casualty_fatal = fatal_df.select("numofcasualties").rdd.flatMap(lambda x: x).collect()
    date_fatal = fatal_df.select("accidentdate").rdd.flatMap(lambda x: x).collect()
    day_fatal = fatal_df.select("day").rdd.flatMap(lambda x: x).collect()
    time_fatal = fatal_df.select("time").rdd.flatMap(lambda x: x).collect()
    loc_fatal = fatal_df.select("location").rdd.flatMap(lambda x: x).collect()
    borough_fatal = fatal_df.select("borough").rdd.flatMap(lambda x: x).collect()

    lat_list_fatal = []
    long_list_fatal = []
    for i in range(len(east_16)):
        lat, long = OSGB36toWGS84(east_16[i], north_16[i])
        lat_list_fatal.append(lat)
        long_list_fatal.append(long)

    serious_df = data_attendent_16.filter(data_attendent_16.severitynum == 2)
    east_16 = serious_df.select("easting").rdd.flatMap(lambda x: x).collect()
    north_16 = serious_df.select("northing").rdd.flatMap(lambda x: x).collect()
    casualty_serious = serious_df.select("numofcasualties").rdd.flatMap(lambda x: x).collect()
    date_serious = serious_df.select("accidentdate").rdd.flatMap(lambda x: x).collect()
    day_serious = serious_df.select("day").rdd.flatMap(lambda x: x).collect()
    time_serious = serious_df.select("time").rdd.flatMap(lambda x: x).collect()
    loc_serious = serious_df.select("location").rdd.flatMap(lambda x: x).collect()
    borough_serious = serious_df.select("borough").rdd.flatMap(lambda x: x).collect()

    lat_list_serious = []
    long_list_serious = []
    for i in range(len(east_16)):
        lat, long = OSGB36toWGS84(east_16[i], north_16[i])
        lat_list_serious.append(lat)
        long_list_serious.append(long)

    """""""""""""""""""""""""""""""""""""""""""""""
    CLUSTER MAP 2016
    """""""""""""""""""""""""""""""""""""""""""""""
    mean_lat = statistics.mean(lat_list_serious)
    mean_long = statistics.mean(long_list_serious)
    acc_map_16 = folium.Map(location=[mean_lat, mean_long], zoom_start=10)
    marker_cluster = MarkerCluster().add_to(acc_map_16)

    for k in range(len(lat_list_fatal)):
        location = lat_list_fatal[k], long_list_fatal[k]
        popup = '<b>FATAL COLLISION, ' + str(casualty_fatal[k]) + ' KILLED</b> <br> <br>On ' + date_fatal[k] + ', ' + \
                day_fatal[k] + ' at ' + time_fatal[k] + ', an accident occured at ' + loc_fatal[k] + ' in ' + \
                borough_fatal[k] + ' killing ' + str(casualty_fatal[k])
        folium.Marker(
            location=location,
            clustered_marker=True,
            popup=folium.Popup(popup, max_width=300, min_width=300),
            icon=folium.Icon(color='red')).add_to(marker_cluster)

    for k in range(len(lat_list_serious)):
        location = lat_list_serious[k], long_list_serious[k]
        popup = '<b>SERIOUS COLLISION, ' + str(casualty_serious[k]) + ' INJURED</b> <br> <br>On ' + date_serious[
            k] + ', ' + \
                day_serious[k] + ' at ' + time_serious[k] + ', an accident occured at ' + loc_serious[k] + ' in ' + \
                borough_serious[k] + ' severely injuring ' + str(casualty_serious[k])
        folium.Marker(
            location=location,
            clustered_marker=True,
            popup=folium.Popup(popup, max_width=300, min_width=300),
            icon=folium.Icon(color='blue')).add_to(marker_cluster)

    acc_map_16.save('assets/map_16.html')

    """""""""""""""""""""""""""""""""""""""""""""""
    LATITUDE LONGITUDE CONVERSION 2015
    """""""""""""""""""""""""""""""""""""""""""""""
    fatal_df = data_attendent_15.filter(data_attendent_15.severitynum == 1)
    east_15 = fatal_df.select("easting").rdd.flatMap(lambda x: x).collect()
    north_15 = fatal_df.select("northing").rdd.flatMap(lambda x: x).collect()
    casualty_fatal = fatal_df.select("numofcasualties").rdd.flatMap(lambda x: x).collect()
    date_fatal = fatal_df.select("accidentdate").rdd.flatMap(lambda x: x).collect()
    day_fatal = fatal_df.select("day").rdd.flatMap(lambda x: x).collect()
    time_fatal = fatal_df.select("time_new").rdd.flatMap(lambda x: x).collect()
    loc_fatal = fatal_df.select("location").rdd.flatMap(lambda x: x).collect()
    borough_fatal = fatal_df.select("borough").rdd.flatMap(lambda x: x).collect()

    lat_list_fatal = []
    long_list_fatal = []
    for i in range(len(east_15)):
        lat, long = OSGB36toWGS84(east_15[i], north_15[i])
        lat_list_fatal.append(lat)
        long_list_fatal.append(long)

    serious_df = data_attendent_15.filter(data_attendent_15.severitynum == 2)
    east_15 = serious_df.select("easting").rdd.flatMap(lambda x: x).collect()
    north_15 = serious_df.select("northing").rdd.flatMap(lambda x: x).collect()
    casualty_serious = serious_df.select("numofcasualties").rdd.flatMap(lambda x: x).collect()
    date_serious = serious_df.select("accidentdate").rdd.flatMap(lambda x: x).collect()
    day_serious = serious_df.select("day").rdd.flatMap(lambda x: x).collect()
    time_serious = serious_df.select("time_new").rdd.flatMap(lambda x: x).collect()
    loc_serious = serious_df.select("location").rdd.flatMap(lambda x: x).collect()
    borough_serious = serious_df.select("borough").rdd.flatMap(lambda x: x).collect()

    lat_list_serious = []
    long_list_serious = []
    for i in range(len(east_15)):
        lat, long = OSGB36toWGS84(east_15[i], north_15[i])
        lat_list_serious.append(lat)
        long_list_serious.append(long)

    """""""""""""""""""""""""""""""""""""""""""""""
    CLUSTER MAP 2015
    """""""""""""""""""""""""""""""""""""""""""""""
    mean_lat = statistics.mean(lat_list_serious)
    mean_long = statistics.mean(long_list_serious)
    acc_map_15 = folium.Map(location=[mean_lat, mean_long], zoom_start=10)
    marker_cluster = MarkerCluster().add_to(acc_map_15)

    for k in range(len(lat_list_fatal)):
        location = lat_list_fatal[k], long_list_fatal[k]
        popup = '<b>FATAL COLLISION, ' + str(casualty_fatal[k]) + ' KILLED</b> <br> <br>On ' + date_fatal[k] + ', ' + \
                day_fatal[k] + ' at ' + time_fatal[k] + ', an accident occured at ' + loc_fatal[k] + ' in ' + \
                borough_fatal[k] + ' killing ' + str(casualty_fatal[k])
        folium.Marker(
            location=location,
            clustered_marker=True,
            popup=folium.Popup(popup, max_width=300, min_width=300),
            icon=folium.Icon(color='red')).add_to(marker_cluster)

    for k in range(len(lat_list_serious)):
        location = lat_list_serious[k], long_list_serious[k]
        popup = '<b>SERIOUS COLLISION, ' + str(casualty_serious[k]) + ' INJURED</b> <br> <br>On ' + date_serious[
            k] + ', ' + \
                day_serious[k] + ' at ' + time_serious[k] + ', an accident occured at ' + loc_serious[k] + ' in ' + \
                borough_serious[k] + ' severely injuring ' + str(casualty_serious[k])
        folium.Marker(
            location=location,
            clustered_marker=True,
            popup=folium.Popup(popup, max_width=300, min_width=300),
            icon=folium.Icon(color='blue')).add_to(marker_cluster)

    acc_map_15.save('assets/map_15.html')

    """""""""""""""""""""""""""""""""""""""""""""""
    LATITUDE LONGITUDE CONVERSION 2014
    """""""""""""""""""""""""""""""""""""""""""""""
    fatal_df = data_attendent_14.filter(data_attendent_14.severitynum == 1)
    east_14 = fatal_df.select("easting").rdd.flatMap(lambda x: x).collect()
    north_14 = fatal_df.select("northing").rdd.flatMap(lambda x: x).collect()
    casualty_fatal = fatal_df.select("numofcasualties").rdd.flatMap(lambda x: x).collect()
    date_fatal = fatal_df.select("accidentdate").rdd.flatMap(lambda x: x).collect()
    day_fatal = fatal_df.select("day").rdd.flatMap(lambda x: x).collect()
    time_fatal = fatal_df.select("time_new").rdd.flatMap(lambda x: x).collect()
    loc_fatal = fatal_df.select("location").rdd.flatMap(lambda x: x).collect()
    borough_fatal = fatal_df.select("borough").rdd.flatMap(lambda x: x).collect()

    lat_list_fatal = []
    long_list_fatal = []
    for i in range(len(east_14)):
        lat, long = OSGB36toWGS84(east_14[i], north_14[i])
        lat_list_fatal.append(lat)
        long_list_fatal.append(long)

    serious_df = data_attendent_14.filter(data_attendent_14.severitynum == 2)
    east_14 = serious_df.select("easting").rdd.flatMap(lambda x: x).collect()
    north_14 = serious_df.select("northing").rdd.flatMap(lambda x: x).collect()
    casualty_serious = serious_df.select("numofcasualties").rdd.flatMap(lambda x: x).collect()
    date_serious = serious_df.select("accidentdate").rdd.flatMap(lambda x: x).collect()
    day_serious = serious_df.select("day").rdd.flatMap(lambda x: x).collect()
    time_serious = serious_df.select("time_new").rdd.flatMap(lambda x: x).collect()
    loc_serious = serious_df.select("location").rdd.flatMap(lambda x: x).collect()
    borough_serious = serious_df.select("borough").rdd.flatMap(lambda x: x).collect()

    lat_list_serious = []
    long_list_serious = []
    for i in range(len(east_14)):
        lat, long = OSGB36toWGS84(east_14[i], north_14[i])
        lat_list_serious.append(lat)
        long_list_serious.append(long)

    """""""""""""""""""""""""""""""""""""""""""""""
    CLUSTER MAP 2014
    """""""""""""""""""""""""""""""""""""""""""""""
    mean_lat = statistics.mean(lat_list_serious)
    mean_long = statistics.mean(long_list_serious)
    acc_map_14 = folium.Map(location=[mean_lat, mean_long], zoom_start=10)
    marker_cluster = MarkerCluster().add_to(acc_map_14)

    for k in range(len(lat_list_fatal)):
        location = lat_list_fatal[k], long_list_fatal[k]
        popup = '<b>FATAL COLLISION, ' + str(casualty_fatal[k]) + ' KILLED</b> <br> <br>On ' + date_fatal[k] + ', ' + \
                day_fatal[k] + ' at ' + time_fatal[k] + ', an accident occured at ' + loc_fatal[k] + ' in ' + \
                borough_fatal[k] + ' killing ' + str(casualty_fatal[k])
        folium.Marker(
            location=location,
            clustered_marker=True,
            popup=folium.Popup(popup, max_width=300, min_width=300),
            icon=folium.Icon(color='red')).add_to(marker_cluster)

    for k in range(len(lat_list_serious)):
        location = lat_list_serious[k], long_list_serious[k]
        popup = '<b>SERIOUS COLLISION, ' + str(casualty_serious[k]) + ' INJURED</b> <br> <br>On ' + date_serious[
            k] + ', ' + \
                day_serious[k] + ' at ' + time_serious[k] + ', an accident occured at ' + loc_serious[k] + ' in ' + \
                borough_serious[k] + ' severely injuring ' + str(casualty_serious[k])
        folium.Marker(
            location=location,
            clustered_marker=True,
            popup=folium.Popup(popup, max_width=300, min_width=300),
            icon=folium.Icon(color='blue')).add_to(marker_cluster)

    acc_map_14.save('assets/map_14.html')


if __name__ == '__main__':
    accidentmap()
