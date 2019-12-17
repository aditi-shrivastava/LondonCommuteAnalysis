import dash
import dash_html_components as html
import dash_core_components as dcc
import pandas as pd
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
from dash.dependencies import Input, Output

nav = Navbar()
app.config['suppress_callback_exceptions']=True

tabs_styles = {
    'height': '33px',
    'margin-left': '160px'
}
tab_style = {
    'borderBottom': '1px solid #d6d6d6',
    'padding': '6px',
    'align' : 'center',
    'fontWeight': 'bold',
    #'margin-left': '10px'
}

tab_selected_style = {
    'borderTop': '1px solid #d6d6d6',
    'borderBottom': '1px solid #d6d6d6',
    'backgroundColor': '#119DFF',
    'color': 'white',
    'align' : 'center',
    'padding': '6px',
   # 'margin-left': '10px'
}

entryexit_17 = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
    .option("dbtable", "entryexit_17") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "anuj_db") \
    .option("password", "anuj_db") \
    .load()

entryexit_16 = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
    .option("dbtable", "entryexit_16") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "anuj_db") \
    .option("password", "anuj_db") \
    .load()

entryexit_15 = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
    .option("dbtable", "entryexit_15") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "anuj_db") \
    .option("password", "anuj_db") \
    .load()

entryexit_14 = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
    .option("dbtable", "entryexit_14") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "anuj_db") \
    .option("password", "anuj_db") \
    .load()

entryexit_13 = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
    .option("dbtable", "entryexit_13") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "anuj_db") \
    .option("password", "anuj_db") \
    .load()

tabdata = dbc.Container(html.Div([
    dcc.Tabs(id="tabs-styled-with-inline", value='2017', children=[
        dcc.Tab(label='2017', value='2017', style=tab_style, selected_style=tab_selected_style),
        dcc.Tab(label='2016', value='2016', style=tab_style, selected_style=tab_selected_style),
        dcc.Tab(label='2015', value='2015', style=tab_style, selected_style=tab_selected_style),
        dcc.Tab(label='2014', value='2014', style=tab_style, selected_style=tab_selected_style),
        dcc.Tab(label='2013', value='2013', style=tab_style, selected_style=tab_selected_style),
    ], style=tabs_styles),
    html.Div(
        [
        dcc.Graph(id='tabs-content-inline'),
        ],className="offset-by-one column"
    )

    ]
)
)

@app.callback(Output('tabs-content-inline', 'figure'),
              [Input('tabs-styled-with-inline', 'value')])
def render_content(tab):

    def heatmap(station, entryweekday, entrysat, entrysun, exitweekday, exitsat, exitsun,year):
        fig = make_subplots(rows=2, cols=1,print_grid=False,shared_yaxes=True,
                            subplot_titles=("No. of people Entering the station in " + str(year), "No. of people Exiting the station in " + str(year)))

        data1 = go.Heatmap(
            z=[entryweekday, entrysat, entrysun],
            x=station,
            # colorscale = 'Viridis',
            y=['Weekday', 'Saturday', 'Sunday'],
            hoverinfo='z')

        fig.append_trace(data1, 1, 1)

        data2 = go.Heatmap(
            z=[exitweekday, exitsat, exitsun],
            x=station,
            # colorscale='Viridis',
            y=['Weekday', 'Saturday', 'Sunday'],
            showscale = False,
            hoverinfo='z')

        fig.append_trace(data2, 2, 1)

        fig.update_layout(height=600, width=1000)

        return fig


    if tab == '2017':
        station_17 = [row['station'] for row in entryexit_17.collect()]
        entryweekday_17 = [row['entryweekday'] for row in entryexit_17.collect()]
        entrysat_17 = [row['entrysaturday'] for row in entryexit_17.collect()]
        entrysun_17 = [row['entrysunday'] for row in entryexit_17.collect()]
        exitweekday_17 = [row['exitweekday'] for row in entryexit_17.collect()]
        exitsat_17 = [row['exitsaturday'] for row in entryexit_17.collect()]
        exitsun_17 = [row['exitsunday'] for row in entryexit_17.collect()]
        ans = heatmap(station_17, entryweekday_17, entrysat_17, entrysun_17, exitweekday_17, exitsat_17, exitsun_17,tab)
        return ans

    elif tab == '2016':
        station_16 = [row['station'] for row in entryexit_16.collect()]
        entryweekday_16 = [row['entryweekday'] for row in entryexit_16.collect()]
        entrysat_16 = [row['entrysaturday'] for row in entryexit_16.collect()]
        entrysun_16 = [row['entrysunday'] for row in entryexit_16.collect()]
        exitweekday_16 = [row['exitweekday'] for row in entryexit_16.collect()]
        exitsat_16 = [row['exitsaturday'] for row in entryexit_16.collect()]
        exitsun_16 = [row['exitsunday'] for row in entryexit_16.collect()]
        ans = heatmap(station_16, entryweekday_16, entrysat_16, entrysun_16, exitweekday_16, exitsat_16, exitsun_16,tab)
        return ans

    elif tab == '2015':
        station_15 = [row['station'] for row in entryexit_15.collect()]
        entryweekday_15 = [row['entryweekday'] for row in entryexit_15.collect()]
        entrysat_15 = [row['entrysaturday'] for row in entryexit_15.collect()]
        entrysun_15 = [row['entrysunday'] for row in entryexit_15.collect()]
        exitweekday_15 = [row['exitweekday'] for row in entryexit_15.collect()]
        exitsat_15 = [row['exitsaturday'] for row in entryexit_15.collect()]
        exitsun_15 = [row['exitsunday'] for row in entryexit_15.collect()]
        ans = heatmap(station_15, entryweekday_15, entrysat_15, entrysun_15, exitweekday_15, exitsat_15, exitsun_15,tab)
        return ans

    elif tab == '2014':
        station_14 = [row['station'] for row in entryexit_14.collect()]
        entryweekday_14 = [row['entryweekday'] for row in entryexit_14.collect()]
        entrysat_14 = [row['entrysaturday'] for row in entryexit_14.collect()]
        entrysun_14 = [row['entrysunday'] for row in entryexit_14.collect()]
        exitweekday_14 = [row['exitweekday'] for row in entryexit_14.collect()]
        exitsat_14 = [row['exitsaturday'] for row in entryexit_14.collect()]
        exitsun_14 = [row['exitsunday'] for row in entryexit_14.collect()]
        ans = heatmap(station_14, entryweekday_14, entrysat_14, entrysun_14, exitweekday_14, exitsat_14, exitsun_14,tab)
        return ans

    elif tab == '2013':
        station_13 = [row['station'] for row in entryexit_13.collect()]
        entryweekday_13 = [row['entryweekday'] for row in entryexit_13.collect()]
        entrysat_13 = [row['entrysaturday'] for row in entryexit_13.collect()]
        entrysun_13 = [row['entrysunday'] for row in entryexit_13.collect()]
        exitweekday_13 = [row['exitweekday'] for row in entryexit_13.collect()]
        exitsat_13 = [row['exitsaturday'] for row in entryexit_13.collect()]
        exitsun_13 = [row['exitsunday'] for row in entryexit_13.collect()]
        ans = heatmap(station_13, entryweekday_13, entrysat_13, entrysun_13, exitweekday_13, exitsat_13, exitsun_13,tab)
        return ans

layout = html.Div([
        nav,
        tabdata,
        ])

