Below we are listing down the data sources for the project. Note that, PostgreSQL is used and the connector postgresql-42.2.8.jar would be required.

## Data Sources
The data sources are CSV files, JSON API's, live bus arrivals and twitter feeds.

https://tfl.gov.uk/info-for/open-data-users/

Username: anuj.saboo  
Password: 1anuj@SFU

Data for Accidents: https://tfl.gov.uk/corporate/publications-and-reports/road-safety  
Data for Tube Usage:http://tfl.gov.uk/tfl/syndication/feeds/counts.zip?app_id=8c71bcb9&app_key=3cbcc5c0f376eea45a0f9be826e07055  

Data for Bus: http://countdown.api.tfl.gov.uk/interfaces/ura/instant_V1?DirectionID=1&returnlist=DirectionID,StopPointName,EstimatedTime,LineName  

Data for Cycling(bi-weekly CSV files): https://cycling.data.tfl.gov.uk/  

Data for Parking Infrastructure:  
Carpark: https://api.tfl.gov.uk/Place/Type/CarPark  
Carpark Occupancy: https://api.tfl.gov.uk/Occupancy/CarPark  
Bike Occupancy: https://api.tfl.gov.uk/BikePoint  

Data from twitter is accessed live

## ETL Execution Instructions
* python3 dataload_Accident.py to execute ETL job for accident data.  
* python3 countsdata.py to execute ETL job for tube usage data.  
* python3 kafkaproducer.py to load data from HTTP and create producer  
* spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 read.py bus to execute Consumer for Kafka stream  
* python3 bus_etl_data.py to execute ETL job for bus data  
* python3 cycledataetl.py to execute ETL job for cycling data  
* python3 Carpark.py to execute ETL job for parking infrastructure data  
* python3 accidentmaps.py to generate Folium Maps  

## Once, the data load has finished, dashboard can be launched using the below command
* Execute python3 index.py to launch the dashboard
