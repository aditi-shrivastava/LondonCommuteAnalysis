import dash
import dash_html_components as html
import dash_core_components as dcc
import pandas as pd
import sys
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import dash_bootstrap_components as dbc
import dash_daq as daq
from navbar import Navbar
from app import app
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, types

spark = SparkSession.builder.appName('Accident Factored').config('spark.driver.extraClassPath','postgresql-42.2.8.jar').getOrCreate()
assert spark.version >= '2.4'  # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext
from dash.dependencies import Input, Output

nav = Navbar()

app.config['suppress_callback_exceptions']=True

borough_count = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "borough_count") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .load()

borough_count.createOrReplaceTempView("borough_count")

day_count = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "day_count") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .load()

day_count.createOrReplaceTempView("day_count")

weather_count = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "weather_count") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .load()

weather_count.createOrReplaceTempView("weather_count")

time_count = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "time_count") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .load()

time_count.createOrReplaceTempView("time_count")

factacc = dbc.Container(
    [
    dbc.Row(
            [
                html.Div
                (
                    [
                html.H4("Factors Affecting Accidents")
                    ],className="offset-by-five column"
                )
            ]
        ),
      dbc.Row(
          [
                    html.Div(
                            [

                                dcc.Graph(id="fig1")

                            ],className="offset-by-two column twelve columns"
                        #,style = {"width": "98%", "display":"inline-block","position":"relative"},className="offset-by-one column nine columns"
                     ),

            html.Div(
                  daq.Slider(
                      id='yearselect',
                      # vertical = True,
                      min=2014,
                      max=2018,
                      size=500,
                      value=2018,
                      handleLabel={"showCurrentValue": True, "label": "Year"},
                      marks={'2014':'2014','2015':'2015','2016':'2016','2017':'2017','2018':'2018',},
                      step=None
                  ), className="offset-by-four columns"
              ),

          ]

      ),

        ]
)

@app.callback(Output('fig1', 'figure'),
              [Input('yearselect', 'value')])
def render_content(year):
    daytab = spark.sql(
        "SELECT daynum,count FROM day_count WHERE "
        "year =" + '"{}"'.format(year) + " ORDER BY daynum")

    timetab = spark.sql(
        "SELECT timeband,count FROM time_count WHERE "
        "year =" + '"{}"'.format(year) + " ORDER BY timeband")

    weathertab = spark.sql(
        "SELECT weather,count FROM weather_count WHERE "
        "year =" + '"{}"'.format(year))

    boroughtab = spark.sql(
        "SELECT borough,count FROM borough_count WHERE "
        "year =" + '"{}"'.format(year) + " ORDER BY count desc limit 7")

    dayx = ['Mon', 'Tue', 'Wed', 'Thur', 'Fri', 'Sat', 'Sun']
    #dayx = daytab.select("daynum").rdd.flatMap(lambda x: x).collect()
    dayy = daytab.select("count").rdd.flatMap(lambda x: x).collect()

    timex = ["00~04", '04~08', '08~12', '12~16', '16~20', '20~24']
    #timex = timetab.select("timeband").rdd.flatMap(lambda x: x).collect()
    timey = timetab.select("count").rdd.flatMap(lambda x: x).collect()

    weatherx = weathertab.select("weather").rdd.flatMap(lambda x: x).collect()
    weathery = weathertab.select("count").rdd.flatMap(lambda x: x).collect()

    boroughx = boroughtab.select("borough").rdd.flatMap(lambda x: x).collect()
    boroughy = boroughtab.select("count").rdd.flatMap(lambda x: x).collect()

    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=("Accidents Vs Days", "Accidents Vs Time", "Accidents Vs Borough", "Accidents Vs Weather"),
        horizontal_spacing=0.12,
        vertical_spacing=0.25,
        specs=[[{"type": "bar"}, {"type": "bar"}],
               [{"type": "bar"}, {"type": "bar"}]]
    )
    fig.add_trace(
        go.Bar(x=dayx, y=dayy),
        row=1, col=1
    )

    fig.add_trace(
        go.Bar(x=timex, y=timey),
        row=1, col=2
    )

    fig.add_trace(
        go.Bar(x=boroughx, y=boroughy),
        row=2, col=1
    )

    fig.add_trace(
        go.Bar(x=weatherx, y=weathery),
        row=2, col=2
    )

    fig.update_layout(showlegend=False,width=900,
        height=500,)

    return fig

layout = html.Div([
        nav,
        factacc,
        ])