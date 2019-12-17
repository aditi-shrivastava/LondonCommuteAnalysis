import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import psycopg2
from pyspark.sql import SparkSession, functions as sf, types
spark = SparkSession.builder.appName('Cycle Data Load').config('spark.driver.extraClassPath','postgresql-42.2.8.jar').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+


cycles = types.StructType([
    types.StructField('Rental Id', types.IntegerType()),
    types.StructField('Duration', types.IntegerType()),
    types.StructField('Bike Id', types.IntegerType()),
    types.StructField('End Date', types.StringType()),
    types.StructField('EndStation Id', types.IntegerType()),
    types.StructField('EndStation Name', types.StringType()),
    types.StructField('Start Date', types.StringType()),
    types.StructField('StartStation Id', types.IntegerType()),
    types.StructField('StartStation Name', types.StringType()),
])

remfiles2019 =  spark.read.option("header","true").schema(cycles).csv("data/cycling/rem2019/*.csv")
allfiles2016 =  spark.read.option("header","true").schema(cycles).csv("data/cycling/2016TripDataZip/*.csv")
allfiles2015 =  spark.read.option("header","true").schema(cycles).csv("data/cycling/2015TripDataZip/*.csv")
allfiles2014 =  spark.read.option("header","true").schema(cycles).csv("data/cycling/cyclehireusagestats-2014/*.csv")
allfiles2013 =  spark.read.option("header","true").schema(cycles).csv("data/cycling/cyclehireusagestats-2013/*.csv")
#allfiles2012 =  spark.read.option("header","true").schema(cycles).csv("cyclehireusagestats-2012/*.csv")


########################2017 data #################################

input_jan2017 = "data/cycling/Jan2017/39JourneyDataExtract04Jan2017-10Jan2017.csv"
input_jan20172 = "data/cycling/Jan2017/40JourneyDataExtract11Jan2017-17Jan2017.csv"
input_jan20173 = "data/cycling/Jan2017/41JourneyDataExtract18Jan2017-24Jan2017.csv"
input_jan20174 = "data/cycling/Jan2017/42JourneyDataExtract25Jan2017-31Jan2017.csv"

input_feb2017 = "data/cycling/Feb2017/43JourneyDataExtract01Feb2017-07Feb2017.csv"
input_feb20172 = "data/cycling/Feb2017/44JourneyDataExtract08Feb2017-14Feb2017.csv"
input_feb20173 = "data/cycling/Feb2017/45JourneyDataExtract15Feb2017-21Feb2017.csv"
input_feb20174 = "data/cycling/Feb2017/46JourneyDataExtract22Feb2017-28Feb2017.csv"

input_mar2017 = "data/cycling/Mar2017/47JourneyDataExtract01Mar2017-07Mar2017.csv"
input_mar20172 = "data/cycling/Mar2017/48JourneyDataExtract08Mar2017-14Mar2017.csv"
input_mar20173 = "data/cycling/Mar2017/49JourneyDataExtract15Mar2017-21Mar2017.csv"
input_mar20174 = "data/cycling/Mar2017/50JourneyData Extract22Mar2017-28Mar2017.csv"

input_apr2017 = "data/cycling/Apr2017/51JourneyDataExtract29Mar2017-04Apr2017.csv"
input_apr20172 = "data/cycling/Apr2017/52JourneyDataExtract05Apr2017-11Apr2017.csv"
input_apr20173 = "data/cycling/Apr2017/53JourneyDataExtract12Apr2017-18Apr2017.csv"
input_apr20174 = "data/cycling/Apr2017/54JourneyDataExtract19Apr2017-25Apr2017.csv"
input_apr20175 = "data/cycling/Apr2017/55JourneyDataExtract26Apr2017-02May2017.csv"

input_may2017 = "data/cycling/May2017/56JourneyDataExtract 03May2017-09May2017.csv"
input_may20172 = "data/cycling/May2017/57JourneyDataExtract10May2017-16May2017.csv"
input_may20173 = "data/cycling/May2017/58JourneyDataExtract17May2017-23May2017.csv"
input_may20174 = "data/cycling/May2017/59JourneyDataExtract24May2017-30May2017.csv"

input_jun2017 = "data/cycling/Jun2017/60JourneyDataExtract31May2017-06Jun2017.csv"
input_jun20172 = "data/cycling/Jun2017/61JourneyDataExtract07Jun2017-13Jun2017.csv"
input_jun20173 = "data/cycling/Jun2017/62JourneyDataExtract14Jun2017-20Jun2017.csv"
input_jun20174 = "data/cycling/Jun2017/63JourneyDataExtract21Jun2017-27Jun2017.csv"

input_jul2017 = "data/cycling/Jul2017/64JourneyDataExtract28Jun2017-04Jul2017.csv"
input_jul20172 = "data/cycling/Jul2017/65JourneyDataExtract05Jul2017-11Jul2017.csv"
input_jul20173 = "data/cycling/Jul2017/67JourneyDataExtract19Jul2017-25Jul2017.csv"
input_jul20174 = "data/cycling/Jul2017/68JourneyDataExtract26Jul2017-31Jul2017.csv"

input_aug2017 = "data/cycling/Aug2017/69JourneyDataExtract01Aug2017-07Aug2017.csv"
input_aug20172 = "data/cycling/Aug2017/70JourneyDataExtract08Aug2017-14Aug2017.csv"
input_aug20173 = "data/cycling/Aug2017/71JourneyDataExtract15Aug2017-22Aug2017.csv"
input_aug20174 = "data/cycling/Aug2017/72JourneyDataExtract23Aug2017-29Aug2017.csv"
input_aug20175 = "data/cycling/Aug2017/73JourneyDataExtract30Aug2017-05Sep2017.csv"

input_sep2017 = "data/cycling/Sep2017/74JourneyDataExtract06Sep2017-12Sep2017.csv"
input_sep20172 = "data/cycling/Sep2017/75JourneyDataExtract13Sep2017-19Sep2017.csv"
input_sep20173 = "data/cycling/Sep2017/76JourneyDataExtract20Sep2017-26Sep2017.csv"
input_sep20174 = "data/cycling/Sep2017/77JourneyDataExtract27Sep2017-03Oct2017.csv"

input_oct2017 = "data/cycling/Oct2017/78JourneyDataExtract04Oct2017-10Oct2017.csv"
input_oct20172 = "data/cycling/Oct2017/79JourneyDataExtract11Oct2017-17Oct2017.csv"
input_oct20173 = "data/cycling/Oct2017/80JourneyDataExtract18Oct2017-24Oct2017.csv"
input_oct20174 = "data/cycling/Oct2017/81JourneyDataExtract25Oct2017-31Oct2017.csv"

input_nov2017 = "data/cycling/Nov2017/82JourneyDataExtract01Nov2017-07Nov2017.csv"
input_nov20172 = "data/cycling/Nov2017/83JourneyDataExtract08Nov2017-14Nov2017.csv"
input_nov20173 = "data/cycling/Nov2017/84JourneyDataExtract15Nov2017-21Nov2017.csv"
input_nov20174 = "data/cycling/Nov2017/85JourneyDataExtract22Nov2017-28Nov2017.csv"


input_dec2017 = "data/cycling/Dec2017/86JourneyDataExtract29Nov2017-05Dec2017.csv"
input_dec20172 = "data/cycling/Dec2017/87JourneyDataExtract06Dec2017-12Dec2017.csv"
input_dec20173 = "data/cycling/Dec2017/88JourneyDataExtract13Dec2017-19Dec2017.csv"
input_dec20174 = "data/cycling/Dec2017/89JourneyDataExtract20Dec2017-26Dec2017.csv"


########################2018 data###################################
input_jan2018 = "data/cycling/Jan2018/90JourneyDataExtract27Dec2017-02Jan2018.csv"
input_jan20182 = "data/cycling/Jan2018/91JourneyDataExtract03Jan2018-09Jan2018.csv"
input_jan20183 = "data/cycling/Jan2018/92JourneyDataExtract10Jan2018-16Jan2018.csv"
input_jan20184 = "data/cycling/Jan2018/93JourneyDataExtract17Jan2018-23Jan2018.csv"
input_jan20185 = "data/cycling/Jan2018/94JourneyDataExtract24Jan2018-30Jan2018.csv"

input_feb2018 = "data/cycling/Feb2018/95JourneyDataExtract31Jan2018-06Feb2018.csv"
input_feb20182 = "data/cycling/Feb2018/96JourneyDataExtract07Feb2018-13Feb2018.csv"
input_feb20183 = "data/cycling/Feb2018/97JourneyDataExtract14Feb2018-20Feb2018.csv"
input_feb20184 = "data/cycling/Feb2018/98JourneyDataExtract21Feb2018-27Feb2018.csv"

input_mar2018 = "data/cycling/Mar2018/99JourneyDataExtract28Feb2018-06Mar2018.csv"
input_mar20182 = "data/cycling/Mar2018/100JourneyDataExtract07Mar2018-13Mar2018.csv"
input_mar20183 = "data/cycling/Mar2018/101JourneyDataExtract14Mar2018-20Mar2018.csv"
input_mar20184 = "data/cycling/Mar2018/102JourneyDataExtract21Mar2018-27Mar2018.csv"

input_apr2018 = "data/cycling/Apr2018/103JourneyDataExtract28Mar2018-03Apr2018.csv"
input_apr20182 = "data/cycling/Apr2018/104JourneyDataExtract04Apr2018-10Apr2018.csv"
input_apr20183 = "data/cycling/Apr2018/105JourneyDataExtract11Apr2018-17Apr2018.csv"
input_apr20184 = "data/cycling/Apr2018/106JourneyDataExtract18Apr2018-24Apr2018.csv"
input_apr20185 = "data/cycling/Apr2018/107JourneyDataExtract25Apr2018-01May2018.csv"

input_may2018 = "data/cycling/May2018/108JourneyDataExtract02May2018-08May2018.csv"
input_may20182 = "data/cycling/May2018/109JourneyDataExtract09May2018-15May2018.csv"
input_may20183 = "data/cycling/May2018/110JourneyDataExtract16May2018-22May2018.csv"
input_may20184 = "data/cycling/May2018/111JourneyDataExtract23May2018-29May2018.csv"

input_jun2018 = "data/cycling/Jun2018/112JourneyDataExtract30May2018-05June2018.csv"
input_jun20182 = "data/cycling/Jun2018/113JourneyDataExtract06June2018-12June2018.csv"
input_jun20183 = "data/cycling/Jun2018/114JourneyDataExtract13June2018-19June2018.csv"
input_jun20184 = "data/cycling/Jun2018/115JourneyDataExtract20June2018-26June2018.csv"

input_jul2018 = "data/cycling/Jul2018/116JourneyDataExtract27June2018-03July2018.csv"
input_jul20182 = "data/cycling/Jul2018/117JourneyDataExtract04July2018-10July2018.csv"
input_jul20183 = "data/cycling/Jul2018/118JourneyDataExtract11July2018-17July2018.csv"
input_jul20184 = "data/cycling/Jul2018/119JourneyDataExtract18July2018-24July2018.csv"
input_jul20185 = "data/cycling/Jul2018/120JourneyDataExtract25July2018-31July2018.csv"

input_aug2018 = "data/cycling/Aug2018/121JourneyDataExtract01Aug2018-07Aug2018.csv"
input_aug20182 = "data/cycling/Aug2018/122JourneyDataExtract08Aug2018-14Aug2018.csv"
input_aug20183 = "data/cycling/Aug2018/123JourneyDataExtract15Aug2018-21Aug2018.csv"
input_aug20184 = "data/cycling/Aug2018/124JourneyDataExtract22Aug2018-28Aug2018.csv"
input_aug20185 = "data/cycling/Aug2018/125JourneyDataExtract29Aug2018-04Sep2018.csv"

input_sep2018 = "data/cycling/Sep2018/126JourneyDataExtract05Sep2018-11Sep2018.csv"
input_sep20182 = "data/cycling/Sep2018/127JourneyDataExtract12Sep2018-18Sep2018.csv"
input_sep20183 = "data/cycling/Sep2018/128JourneyDataExtract19Sep2018-25Sep2018.csv"
input_sep20184 = "data/cycling/Sep2018/129JourneyDataExtract26Sep2018-02Oct2018.csv"

input_oct2018 = "data/cycling/Oct2018/130JourneyDataExtract03Oct2018-09Oct2018.csv"
input_oct20182 = "data/cycling/Oct2018/131JourneyDataExtract10Oct2018-16Oct2018.csv"
input_oct20183 = "data/cycling/Oct2018/132JourneyDataExtract17Oct2018-23Oct2018.csv"
input_oct20184 = "data/cycling/Oct2018/133JourneyDataExtract24Oct2018-30Oct2018.csv"

input_nov2018 = "data/cycling/Nov2018/134JourneyDataExtract31Oct2018-06Nov2018.csv"
input_nov20182 = "data/cycling/Nov2018/135JourneyDataExtract07Nov2018-13Nov2018.csv"
input_nov20183 = "data/cycling/Nov2018/136JourneyDataExtract14Nov2018-20Nov2018.csv"
input_nov20184 = "data/cycling/Nov2018/137JourneyDataExtract21Nov2018-27Nov2018.csv"
input_nov20185 = "data/cycling/Nov2018/138JourneyDataExtract28Nov2018-04Dec2018.csv"

input_dec2018 = "data/cycling/Dec2018/139JourneyDataExtract05Dec2018-11Dec2018.csv"
input_dec20182 = "data/cycling/Dec2018/140JourneyDataExtract12Dec2018-18Dec2018.csv"
input_dec20183 = "data/cycling/Dec2018/141JourneyDataExtract19Dec2018-25Dec2018.csv"
input_dec20184 = "data/cycling/Dec2018/142JourneyDataExtract26Dec2018-01Jan2019.csv"

#####################2019 data##################################
input_jan = "data/cycling/Jan/143JourneyDataExtract02Jan2019-08Jan2019.csv"
input_jan1 = "data/cycling/Jan/144JourneyDataExtract09Jan2019-15Jan2019.csv"
input_jan2 = "data/cycling/Jan/145JourneyDataExtract16Jan2019-22Jan2019.csv"
input_jan3 = "data/cycling/Jan/146JourneyDataExtract23Jan2019-29Jan2019.csv"
input_jan4 = "data/cycling/Jan/147JourneyDataExtract30Jan2019-05Feb2019.csv"

input_feb = "data/cycling/Feb/148JourneyDataExtract06Feb2019-12Feb2019.csv"
input_feb1 = "data/cycling/Feb/149JourneyDataExtract13Feb2019-19Feb2019.csv"
input_feb2 = "data/cycling/Feb/150JourneyDataExtract20Feb2019-26Feb2019.csv"
input_feb3 = "data/cycling/Feb/151JourneyDataExtract27Feb2019-05Mar2019.csv"

input_mar = "data/cycling/Mar/152JourneyDataExtract06Mar2019-12Mar2019.csv"
input_mar1 = "data/cycling/Mar/153JourneyDataExtract13Mar2019-19Mar2019.csv"
input_mar2 = "data/cycling/Mar/154JourneyDataExtract20Mar2019-26Mar2019.csv"
input_mar3 = "data/cycling/Mar/155JourneyDataExtract27Mar2019-02Apr2019.csv"

input_apr = "data/cycling/April/156JourneyDataExtract03Apr2019-09Apr2019.csv"
input_apr1 = "data/cycling/April/157JourneyDataExtract10Apr2019-16Apr2019.csv"
input_apr2 = "data/cycling/April/158JourneyDataExtract17Apr2019-23Apr2019.csv"
input_apr3 = "data/cycling/April/159JourneyDataExtract24Apr2019-30Apr2019.csv"

input_may = "data/cycling/May/160JourneyDataExtract01May2019-07May2019.csv"
input_may1 = "data/cycling/May/161JourneyDataExtract08May2019-14May2019.csv"
input_may2 = "data/cycling/May/162JourneyDataExtract15May2019-21May2019.csv"
input_may3 = "data/cycling/May/163JourneyDataExtract22May2019-28May2019.csv"
input_may4= "data/cycling/May/164JourneyDataExtract29May2019-04Jun2019.csv"


inputs='/data/cycling/170JourneyDataExtract10Jul2019-16Jul2019.csv'
inputs1='/data/cycling/171JourneyDataExtract17Jul2019-23Jul2019.csv'
inputs2='/data/cycing/172JourneyDataExtract24Jul2019-30Jul2019.csv'


data = spark.read.csv(input_jan, schema=cycles, header=True)
data1 = spark.read.csv(input_jan1, schema=cycles, header=True)
data2 = spark.read.csv(input_jan2, schema=cycles, header=True)
data3 = spark.read.csv(input_jan3, schema=cycles, header=True)
data4 = spark.read.csv(input_jan4, schema=cycles, header=True)
data5 = spark.read.csv(input_feb, schema=cycles, header=True)
data6 = spark.read.csv(input_feb1, schema=cycles, header=True)
data7 = spark.read.csv(input_feb2, schema=cycles, header=True)
data8 = spark.read.csv(input_feb3, schema=cycles, header=True)
data9 = spark.read.csv(input_mar, schema=cycles, header=True)
data10 = spark.read.csv(input_mar1, schema=cycles, header=True)
data11 = spark.read.csv(input_mar2, schema=cycles, header=True)
data12 = spark.read.csv(input_mar3, schema=cycles, header=True)
data13 = spark.read.csv(input_apr, schema=cycles, header=True)
data14 = spark.read.csv(input_apr1, schema=cycles, header=True)
data15 = spark.read.csv(input_apr2, schema=cycles, header=True)
data16 = spark.read.csv(input_apr3, schema=cycles, header=True)
data17 = spark.read.csv(input_may, schema=cycles, header=True)
data18 = spark.read.csv(input_may1, schema=cycles, header=True)
data19 = spark.read.csv(input_may2, schema=cycles, header=True)
data20 = spark.read.csv(input_may3, schema=cycles, header=True)
data21 = spark.read.csv(input_may4, schema=cycles, header=True)

data22 = spark.read.csv(input_jan2018, schema=cycles, header=True)
data23 = spark.read.csv(input_jan20182, schema=cycles, header=True)
data24 = spark.read.csv(input_jan20183, schema=cycles, header=True)
data25 = spark.read.csv(input_jan20184, schema=cycles, header=True)
data26 = spark.read.csv(input_jan20185, schema=cycles, header=True)
data27 = spark.read.csv(input_feb2018, schema=cycles, header=True)
data28 = spark.read.csv(input_feb20182, schema=cycles, header=True)
data29 = spark.read.csv(input_feb20183, schema=cycles, header=True)
data30 = spark.read.csv(input_feb20184, schema=cycles, header=True)
data31 = spark.read.csv(input_mar2018, schema=cycles, header=True)
data32 = spark.read.csv(input_mar20182, schema=cycles, header=True)
data33 = spark.read.csv(input_mar20183, schema=cycles, header=True)
data34 = spark.read.csv(input_mar20184, schema=cycles, header=True)
data35 = spark.read.csv(input_apr2018, schema=cycles, header=True)
data36 = spark.read.csv(input_apr20182, schema=cycles, header=True)
data37 = spark.read.csv(input_apr20183, schema=cycles, header=True)
data38 = spark.read.csv(input_apr20184, schema=cycles, header=True)
data39 = spark.read.csv(input_apr20185, schema=cycles, header=True)
data40 = spark.read.csv(input_may2018, schema=cycles, header=True)
data41 = spark.read.csv(input_may20182, schema=cycles, header=True)
data42 = spark.read.csv(input_may20183, schema=cycles, header=True)
data43 = spark.read.csv(input_may20184, schema=cycles, header=True)
data44 = spark.read.csv(input_jun2018, schema=cycles, header=True)
data45 = spark.read.csv(input_jun20182, schema=cycles, header=True)
data46 = spark.read.csv(input_jun20183, schema=cycles, header=True)
data47 = spark.read.csv(input_jun20184, schema=cycles, header=True)
data48 = spark.read.csv(input_jul2018, schema=cycles, header=True)
data49 = spark.read.csv(input_jul20182, schema=cycles, header=True)
data50 = spark.read.csv(input_jul20183, schema=cycles, header=True)
data51 = spark.read.csv(input_jul20184, schema=cycles, header=True)
data52 = spark.read.csv(input_jul20185, schema=cycles, header=True)
data53 = spark.read.csv(input_aug2018, schema=cycles, header=True)
data54 = spark.read.csv(input_aug20182, schema=cycles, header=True)
data55 = spark.read.csv(input_aug20183, schema=cycles, header=True)
data56 = spark.read.csv(input_aug20184, schema=cycles, header=True)
data57 = spark.read.csv(input_aug20185, schema=cycles, header=True)
data58 = spark.read.csv(input_sep2018, schema=cycles, header=True)
data59 = spark.read.csv(input_sep20182, schema=cycles, header=True)
data60 = spark.read.csv(input_sep20183, schema=cycles, header=True)
data61 = spark.read.csv(input_sep20184, schema=cycles, header=True)
data62 = spark.read.csv(input_oct2018, schema=cycles, header=True)
data63 = spark.read.csv(input_oct20182, schema=cycles, header=True)
data64 = spark.read.csv(input_oct20183, schema=cycles, header=True)
data65 = spark.read.csv(input_oct20184, schema=cycles, header=True)
data66 = spark.read.csv(input_nov2018, schema=cycles, header=True)
data67 = spark.read.csv(input_nov20182, schema=cycles, header=True)
data68 = spark.read.csv(input_nov20183, schema=cycles, header=True)
data69 = spark.read.csv(input_nov20184, schema=cycles, header=True)
data70 = spark.read.csv(input_nov20185, schema=cycles, header=True)
data71 = spark.read.csv(input_dec2018, schema=cycles, header=True)
data72 = spark.read.csv(input_dec20182, schema=cycles, header=True)
data73 = spark.read.csv(input_dec20183, schema=cycles, header=True)
data74 = spark.read.csv(input_dec20184, schema=cycles, header=True)


data116	=	spark.read.csv(input_jan2017,	schema=cycles,	header=True)
data117	=	spark.read.csv(input_jan20172,	schema=cycles,	header=True)
data118	=	spark.read.csv(input_jan20173,	schema=cycles,	header=True)
data119	=	spark.read.csv(input_jan20174,	schema=cycles,	header=True)
data120	=	spark.read.csv(input_feb2017,	schema=cycles,	header=True)
data121	=	spark.read.csv(input_feb20172,	schema=cycles,	header=True)
data122	=	spark.read.csv(input_feb20173,	schema=cycles,	header=True)
data123	=	spark.read.csv(input_feb20174,	schema=cycles,	header=True)
data124	=	spark.read.csv(input_mar2017,	schema=cycles,	header=True)
data75	=	spark.read.csv(input_mar20172,	schema=cycles,	header=True)
data76	=	spark.read.csv(input_mar20173,	schema=cycles,	header=True)
data77	=	spark.read.csv(input_mar20174,	schema=cycles,	header=True)
data78	=	spark.read.csv(input_apr2017,	schema=cycles,	header=True)
data79	=	spark.read.csv(input_apr20172,	schema=cycles,	header=True)
data80	=	spark.read.csv(input_apr20173,	schema=cycles,	header=True)
data81	=	spark.read.csv(input_apr20174,	schema=cycles,	header=True)
data82	=	spark.read.csv(input_apr20175,	schema=cycles,	header=True)
data83	=	spark.read.csv(input_may2017,	schema=cycles,	header=True)
data84	=	spark.read.csv(input_may20172,	schema=cycles,	header=True)
data85	=	spark.read.csv(input_may20173,	schema=cycles,	header=True)
data86	=	spark.read.csv(input_may20174,	schema=cycles,	header=True)
data87	=	spark.read.csv(input_jun2017,	schema=cycles,	header=True)
data88	=	spark.read.csv(input_jun20172,	schema=cycles,	header=True)
data89	=	spark.read.csv(input_jun20173,	schema=cycles,	header=True)
data90	=	spark.read.csv(input_jun20174,	schema=cycles,	header=True)
data91	=	spark.read.csv(input_jul2017,	schema=cycles,	header=True)
data92	=	spark.read.csv(input_jul20172,	schema=cycles,	header=True)
data93	=	spark.read.csv(input_jul20173,	schema=cycles,	header=True)
data94	=	spark.read.csv(input_jul20174,	schema=cycles,	header=True)
data95	=	spark.read.csv(input_aug2017,	schema=cycles,	header=True)
data96	=	spark.read.csv(input_aug20172,	schema=cycles,	header=True)
data97	=	spark.read.csv(input_aug20173,	schema=cycles,	header=True)
data98	=	spark.read.csv(input_aug20174,	schema=cycles,	header=True)
data99	=	spark.read.csv(input_aug20175,	schema=cycles,	header=True)
data100	=	spark.read.csv(input_sep2017,	schema=cycles,	header=True)
data101	=	spark.read.csv(input_sep20172,	schema=cycles,	header=True)
data102	=	spark.read.csv(input_sep20173,	schema=cycles,	header=True)
data103	=	spark.read.csv(input_sep20174,	schema=cycles,	header=True)
data104	=	spark.read.csv(input_oct2017,	schema=cycles,	header=True)
data105	=	spark.read.csv(input_oct20172,	schema=cycles,	header=True)
data106	=	spark.read.csv(input_oct20173,	schema=cycles,	header=True)
data107	=	spark.read.csv(input_oct20174,	schema=cycles,	header=True)
data108	=	spark.read.csv(input_nov2017,	schema=cycles,	header=True)
data109	=	spark.read.csv(input_nov20172,	schema=cycles,	header=True)
data110	=	spark.read.csv(input_nov20173,	schema=cycles,	header=True)
data111	=	spark.read.csv(input_nov20174,	schema=cycles,	header=True)
data112	=	spark.read.csv(input_dec2017,	schema=cycles,	header=True)
data113	=	spark.read.csv(input_dec20172,	schema=cycles,	header=True)
data114	=	spark.read.csv(input_dec20173,	schema=cycles,	header=True)
data115	=	spark.read.csv(input_dec20174,	schema=cycles,	header=True)

data = data.union(data1)
data = data.union(data2)
data = data.union(data3)
data = data.union(data4)
data = data.union(data5)
data = data.union(data6)
data = data.union(data7)
data = data.union(data8)
data = data.union(data9)
data = data.union(data10)
data = data.union(data11)
data = data.union(data12)
data = data.union(data13)
data = data.union(data14)
data = data.union(data15)
data = data.union(data16)
data = data.union(data17)
data = data.union(data18)
data = data.union(data19)
data = data.union(data20)
data = data.union(data21)

data = data.union(data22)
data = data.union(data23)
data = data.union(data24)
data = data.union(data25)
data = data.union(data26)
data = data.union(data27)
data = data.union(data28)
data = data.union(data29)
data = data.union(data30)
data = data.union(data31)
data = data.union(data32)
data = data.union(data33)
data = data.union(data34)
data = data.union(data35)
data = data.union(data36)
data = data.union(data37)
data = data.union(data38)
data = data.union(data39)
data = data.union(data40)
data = data.union(data41)
data = data.union(data42)
data = data.union(data43)
data = data.union(data44)
data = data.union(data45)
data = data.union(data46)
data = data.union(data47)
data = data.union(data48)
data = data.union(data49)
data = data.union(data50)
data = data.union(data51)
data = data.union(data52)
data = data.union(data53)
data = data.union(data54)
data = data.union(data55)
data = data.union(data56)
data = data.union(data57)
data = data.union(data58)
data = data.union(data59)
data = data.union(data60)
data = data.union(data61)
data = data.union(data62)
data = data.union(data63)
data = data.union(data64)
data = data.union(data65)

data = data.union(data66)
data = data.union(data67)
data = data.union(data68)
data = data.union(data69)
data = data.union(data70)
data = data.union(data71)
data = data.union(data72)
data = data.union(data73)
data = data.union(data74)
data = data.union(data75)
data = data.union(data76)
data = data.union(data77)
data = data.union(data78)
data = data.union(data79)
data = data.union(data80)
data = data.union(data81)
data = data.union(data82)
data = data.union(data83)
data = data.union(data84)
data = data.union(data85)
data = data.union(data86)
data = data.union(data87)
data = data.union(data88)
data = data.union(data89)
data = data.union(data90)
data = data.union(data91)
data = data.union(data92)
data = data.union(data93)
data = data.union(data94)
data = data.union(data95)
data = data.union(data96)
data = data.union(data97)
data = data.union(data98)
data = data.union(data99)
data = data.union(data100)
data = data.union(data101)
data = data.union(data102)
data = data.union(data103)
data = data.union(data104)
data = data.union(data105)
data = data.union(data106)
data = data.union(data107)
data = data.union(data108)
data = data.union(data109)
data = data.union(data110)
data = data.union(data111)
data = data.union(data112)
data = data.union(data113)
data = data.union(data114)
data = data.union(data115)
data = data.union(data116)
data = data.union(data117)
data = data.union(data118)
data = data.union(data119)
data = data.union(data120)
data = data.union(data121)
data = data.union(data122)
data = data.union(data123)
data = data.union(data124)

data = data.union(allfiles2016)
data = data.union(allfiles2015)
data = data.union(allfiles2014)
data = data.union(allfiles2013)
#data = data.union(allfiles2012)
data = data.union(remfiles2019)

# print(data.count())


#select SUM(CAST("Duration" as INTEGER)),"StartStation Name","EndStation Name" from cycles group by "StartStation Name","EndStation Name" order by SUM DESC LIMIT 100;
#select to_date("End Date",'dd/mm/yyyy') from cycles;
#select AVG(CAST("Duration" as INTEGER)) as abc,to_date("End_date",'dd/mm/yyyy') as dt from cycles group by to_date("End_date",'dd/mm/yyyy') order by dt LIMIT 50;

data= data.withColumnRenamed('End Date', 'End_date').withColumnRenamed('Start Date', 'Start_date').withColumnRenamed('EndStation Name', 'EndStation_Name').withColumnRenamed('StartStation Name', 'StartStation_Name')\
.withColumnRenamed('StartStation Id', 'StartStation_Id').withColumnRenamed('StartStation Id', 'StartStation_Id')
# data = data.withColumn("date",data.End_date.substr(0, 10))


data = data.withColumn('End_date', sf.concat(data.End_date.substr(7,4),sf.lit('-'), data.End_date.substr(4,2),sf.lit('-'),data.End_date.substr(1,2)))
data = data.withColumn('Start_date', sf.concat(data.Start_date.substr(7,4),sf.lit('-'), data.Start_date.substr(4,2),sf.lit('-'),data.Start_date.substr(1,2)))

# data = data.withColumn('Start_date', (data.Start_date.substr(0, 10)))
data.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
    .option("dbtable", "cycle_all") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "anuj_db") \
    .option("password","anuj_db")\
    .save()

connection = psycopg2.connect(user="anuj_db",
                                  password="anuj_db",
                                  host="localhost",
                                  port="5432",
                                  database="anuj_db")

cursor = connection.cursor()

query_list = ["CREATE TABLE IF NOT EXISTS cyclefinal (count int, dt text)",
              "Truncate table cyclefinal",
              "insert into cyclefinal select count(1) as count,"'"End_date"'"  as dt from cycle_all group by "'"End_date"'" order by dt",
              "Delete from cyclefinal where count in (select count from cyclefinal order by count desc limit 2)"]

if (connection):
    cursor = connection.cursor()
    print("PostgreSQL connection is opened")
    count=0
    for q in query_list:
        count+=1
        cursor.execute(q)
        connection.commit()
        print("Query "+str(count)+" executed ")
cursor.close()
connection.close()
print("PostgreSQL connection is closed")




#select AVG("Duration") as duration,"StartStation_Name","StartStation_Id"  as station from cycles group by "StartStation_Name","StartStation_Id" order by station limit 10;
#insert into cyclestation select AVG("Duration") as abc,"StartStation_Name","StartStation_Id"  as station from cycles group by "StartStation_Name","StartStation_Id" order by station limit 10;


#insert into cycleduration select AVG("Duration") as duration, "End_Date"  as dt from cycle_full group by "End_date" order by dt;


# select b.name,b.lat,b.lon from bikepark b Inner Join cyclescount c ON b.name=c.station_nm LIMIT 10;
#
# select AVG("Duration") as duration,"StartStation_Name","StartStation_Id"  as station from cycles group by "StartStation_Name","StartStation_Id" order by station limit 10;
#
# #insert into cyclefinal select count(1) as count,"End_date"  as dt from cycles group by "End_date" order by dt;
#
# insert into cyclecount select count(1) as count,"StartStation_Name"  as stn from cycles group by "StartStation_Name" order by stn;
#



#CREATE TABLE IF NOT EXISTS cyclefinal (count int, dt text)
#Truncate table cyclefinal
#insert into cyclefinal select count(1) as count,"End_date"  as dt from cycles group by "End_date" order by dt;
#delete from cyclefinal where count=(select max(count) from cyclefinal)
