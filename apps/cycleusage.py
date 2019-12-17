import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Output, Input
import dash_bootstrap_components as dbc
from navbar import Navbar
from pyspark.sql import SparkSession
from app import app
import plotly.graph_objs as go

app.config['suppress_callback_exceptions']=True
spark = SparkSession.builder.appName('Accident Analysis').config('spark.driver.extraClassPath','postgresql-42.2.8.jar').getOrCreate()

nav = Navbar()

cyclefin = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "cyclefinal") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .load()

def plotfig(count,date):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=date, y=count, name="Count", line_color='deepskyblue'))

    fig.update_layout(
                width=1000,
                height=600,
                title={'text': "Time Series of Cycle Usage",
               'x': 0.5,
               'y': 0.9,
               'xanchor': 'center',
               'yanchor': 'top'},
                xaxis_rangeslider_visible=True,
                      )
    return fig

count = cyclefin.select("count").rdd.flatMap(lambda x: x).collect()
date = cyclefin.select("dt").rdd.flatMap(lambda x: x).collect()
ans = plotfig(count,date)

cycfig = dbc.Container\
(
    html.Div
        (
            [
            dcc.Graph(id='dotmap',figure=ans)
            ],className="offset-by-one column"
        )
)

layout = html.Div([
        nav,
        cycfig,
        ])