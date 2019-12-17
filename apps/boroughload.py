import dash_html_components as html
import dash_core_components as dcc
import sys
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import dash_bootstrap_components as dbc
from navbar import Navbar
from app import app
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, types

spark = SparkSession.builder.appName('Train Data Analysis').config('spark.driver.extraClassPath','postgresql-42.2.8.jar').getOrCreate()
assert spark.version >= '2.4'  # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

nav = Navbar()
#app = dash.Dash()
app.config['suppress_callback_exceptions']=True

netload_per_borough_17 = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "netload_per_borough_17") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .load()

netload_per_borough_16 = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
    .option("dbtable", "netload_per_borough_16") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "anuj_db") \
    .option("password", "anuj_db") \
    .load()

netload_per_borough_15 = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
    .option("dbtable", "netload_per_borough_15") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "anuj_db") \
    .option("password", "anuj_db") \
    .load()

netload_per_borough_14 = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
    .option("dbtable", "netload_per_borough_14") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "anuj_db") \
    .option("password", "anuj_db") \
    .load()

netload_per_borough_13 = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
    .option("dbtable", "netload_per_borough_13") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "anuj_db") \
    .option("password", "anuj_db") \
    .load()


boroughlist = [row['borough'] for row in netload_per_borough_17.collect()]

netload_17 = [row['sum(entryexitinmillion)'] for row in netload_per_borough_17.collect()]
netload_16 = [row['sum(entryexitinmillion)'] for row in netload_per_borough_16.collect()]
netload_15 = [row['sum(entryexitinmillion)'] for row in netload_per_borough_15.collect()]
netload_14 = [row['sum(entryexitinmillion)'] for row in netload_per_borough_14.collect()]
netload_13 = [row['sum(entryexitinmillion)'] for row in netload_per_borough_13.collect()]

def dotplot(boroughlist, netload_13, netload_14, netload_15, netload_16, netload_17):
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=netload_13,
        y=boroughlist,
        marker=dict(color="violet", size=8),
        mode="markers",
        name="2013",
    ))

    fig.add_trace(go.Scatter(
        x=netload_14,
        y=boroughlist,
        marker=dict(color="cyan", size=8),
        mode="markers",
        name="2014",
    ))

    fig.add_trace(go.Scatter(
        x=netload_15,
        y=boroughlist,
        marker=dict(color="gold", size=8),
        mode="markers",
        name="2015",
    ))

    fig.add_trace(go.Scatter(
        x=netload_16,
        y=boroughlist,
        marker=dict(color="purple", size=8),
        mode="markers",
        name="2016",
    ))

    fig.add_trace(go.Scatter(
        x=netload_17,
        y=boroughlist,
        marker=dict(color="crimson", size=8),
        mode="markers",
        name="2017",
    ))

    fig.update_layout(
        autosize=False,
        width=1100,
        height=700,
        title={'text': "Distribution of Load per Borough over the Years(2013-2017)",
               'x': 0.5,
               'y': 0.9,
               'xanchor': 'center',
               'yanchor': 'top'},
        xaxis_title="Net load per borough (in millions)",
        yaxis_title="Boroughs"
    )
    return fig


ans = dotplot(boroughlist, netload_13, netload_14, netload_15, netload_16, netload_17)

dotfig = dbc.Container\
(
    html.Div
        (
            [
            dcc.Graph(id='dotmap',figure=ans)
            ]
        )

)


layout = html.Div([
        nav,
        dotfig,
        ])