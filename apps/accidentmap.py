import dash_html_components as html
from dash.dependencies import Output, Input
import dash_bootstrap_components as dbc
import dash_daq as daq
from navbar import Navbar
from pyspark.sql import SparkSession
from app import app

app.config['suppress_callback_exceptions']=True
spark = SparkSession.builder.appName('Parking Analysis').config('spark.driver.extraClassPath','postgresql-42.2.8.jar').getOrCreate()

nav = Navbar()

map = dbc.Container(
    [
    dbc.Row(
            [
                html.Div
                (
                    [
                html.H4("ACCIDENT LOCATIONS")
                    ],className="offset-by-four column"
                )
            ]
        ),
      dbc.Row(
          [
              dbc.Col(
                        html.Div(
                            [

                                html.Div(id="mapoutput")

                            ]
                        ),style = {"width": "98%", "display":"inline-block","position":"relative"},className="offset-by-one column nine columns"
                     ),

                  html.Div(
                  daq.Slider(
                        id='year-slider',
                        vertical = True,
                        min=2014,
                        max=2018,
                        size = 500,
                        value=2014,
                        handleLabel={"showCurrentValue": True,"label": "Year"},
                        marks={str(year): str(year) for year in (2014,2015,2016,2017,2018)},
                        step=None
                      ),style = {"width": "2%", "display":"inline-block","position":"relative"}
                )
          ]

      ),

        ]
)

@app.callback(
    Output('mapoutput', 'children'),
    [Input('year-slider', 'value')])
def update_figure(year):
    if year == 2014:
        return html.Iframe(id='map',srcDoc=open('/home/anuj/PycharmProjects/732/Project/assets/map_14.html','r').read(), width='100%', height='500')
    elif year == 2015:
        return html.Iframe(id='map',
                           srcDoc=open('/home/anuj/PycharmProjects/732/Project/assets/map_15.html', 'r').read(),
                           width='100%', height='500')
    elif year == 2016:
        return html.Iframe(id='map',
                           srcDoc=open('/home/anuj/PycharmProjects/732/Project/assets/map_16.html', 'r').read(),
                           width='100%', height='500')
    elif year == 2017:
        return html.Iframe(id='map',
                           srcDoc=open('/home/anuj/PycharmProjects/732/Project/assets/map_17.html', 'r').read(),
                           width='100%', height='500')
    else:
        return html.Iframe(id='map',
                           srcDoc=open('/home/anuj/PycharmProjects/732/Project/assets/map_18.html', 'r').read(),
                           width='100%', height='500')

layout = html.Div([
        nav,
        map,
        ])

