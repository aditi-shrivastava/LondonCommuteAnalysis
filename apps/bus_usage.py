import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Output, Input
import dash_bootstrap_components as dbc
from navbar import Navbar
from pyspark.sql import SparkSession
from app import app
import plotly.graph_objs as go
import datetime
import pandas as pd

app.config['suppress_callback_exceptions']=True
spark = SparkSession.builder.appName('Bus Stream').config('spark.driver.extraClassPath','postgresql-42.2.8.jar').getOrCreate()

nav = Navbar()

data3 = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
        .option("dbtable", "bus_all") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "anuj_db") \
        .option("password", "anuj_db") \
        .load()

def perf():
    x1 = list(range(50))

    date_time_str = '2019-12-04 09:45:00'
    Today = datetime.datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S')
    date_list = [Today + datetime.timedelta(minutes=8 * x) for x in range(0, 50)]
    diff1 = [x.strftime('%Y-%m-%d %H:%M:%S') for x in date_list]

    diff2 = data3.select("Arrival_Time").rdd.flatMap(lambda x: x).sortBy(lambda x: x).collect()

    x2 = list(range(50))

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=x1, y=diff1,
                             mode='lines+markers',
                             name='Expected timing'))
    fig.add_trace(go.Scatter(x=x2, y=diff2,
                             mode='lines',
                             name='Live timing'))
    fig.update_layout(title_text='Expected Vs Live bus arrival timing for Bus 16 at Paul Street Station')
    fig.update_layout(
        autosize=False,
        width=1000,
        height=450,
        title={'text': "Expected Vs Live bus arrival timing for Bus 16 at Paul Street Station",
               'x': 0.5,
               'y': 0.9,
               'xanchor': 'center',
               'yanchor': 'top'},
                    )
    return fig

def delay():
    delay = data3.select("delay").rdd.flatMap(lambda x: x).collect()
    time = data3.select("Arrival_Time").rdd.flatMap(lambda x: x).collect()

    delay = delay[1:]
    delay = [-x for x in delay]
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

    bars = []
    for label, label_df in df.groupby('label'):
        bars.append(go.Bar(x=label_df.x,
                           y=label_df.y,
                           name=label,
                           marker={'color': colors[label]}))

    figure = go.Figure(data=bars)
    f2 = go.FigureWidget(figure)
    #f2.layout.title = 'Bus arrival Delay for Bus 16 at Paul Street Station'
    f2.update_layout(
        autosize=False,
        width=1000,
        height=450,
        title={'text': "Bus arrival Delay for Bus 16 at Paul Street Station",
               'x': 0.5,
               'y': 0.9,
               'xanchor': 'center',
               'yanchor': 'top'},
    )
    return f2


g1 = perf()
g2 = delay()

busdata = dbc.Container(
    [
    dbc.Row(
        [
           html.Div(
                [
                dcc.Graph(id='graph1',figure=g1),
                ],className="offset-by-one column"

            )
        ]
    ),
    dbc.Row(
        [

                html.Div(
                [
                dcc.Graph(id='graph2',figure=g2),
                ],className="offset-by-one column"
                            )

        ]
    )
]
)


layout = html.Div([
        nav,
        busdata,
        ])
