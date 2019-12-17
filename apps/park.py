import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Output, Input
import dash_bootstrap_components as dbc
from navbar import Navbar
from pyspark.sql import SparkSession
from app import app
import plotly.graph_objs as go
import dash_daq as daq
app.config['suppress_callback_exceptions']=True
spark = SparkSession.builder.appName('Parking Analysis').config('spark.driver.extraClassPath','postgresql-42.2.8.jar').getOrCreate()

nav = Navbar()
mapbox_access_token = "pk.eyJ1IjoiYW51amNoYW1wMTciLCJhIjoiY2szOHp5YzU1MGYyODNjcDExbmQwN3cwdCJ9.h9oIb9wcULGnDOFEuWU5kA"

table_carpark = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
    .option("dbtable", "carpark") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "anuj_db") \
    .option("password", "anuj_db") \
    .load()

table_bikepark = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
    .option("dbtable", "bikepark") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "anuj_db") \
    .option("password", "anuj_db") \
    .load()

table_carpark.createOrReplaceTempView('table_carpark')

table_bikepark.createOrReplaceTempView('table_bikepark')

carpark_disabled = spark.sql("SELECT * FROM table_carpark WHERE baytype = 'Disabled'")
carpark_pay = spark.sql("SELECT * FROM table_carpark WHERE baytype = 'Pay and Display Parking'")

carpark_lat = carpark_disabled.select("lat").rdd.flatMap(lambda x: x).collect()
carpark_lon = carpark_disabled.select("lon").rdd.flatMap(lambda x: x).collect()
carpark_name = carpark_disabled.select("name").rdd.flatMap(lambda x: x).collect()

dis_count = carpark_disabled.select("baycount").rdd.flatMap(lambda x: x).collect()
dis_occupied = carpark_disabled.select("occupied").rdd.flatMap(lambda x: x).collect()
dis_free = carpark_disabled.select("free").rdd.flatMap(lambda x: x).collect()

pay_count = carpark_pay.select("baycount").rdd.flatMap(lambda x: x).collect()
pay_occupied =carpark_pay.select("occupied").rdd.flatMap(lambda x: x).collect()
pay_free =carpark_pay.select("free").rdd.flatMap(lambda x: x).collect()

bikepark_name =table_bikepark.select("name").rdd.flatMap(lambda x: x).collect()
bikepark_lat =table_bikepark.select("lat").rdd.flatMap(lambda x: x).collect()
bikepark_lon =table_bikepark.select("lon").rdd.flatMap(lambda x: x).collect()
bikepark_docks =table_bikepark.select("ndocks").rdd.flatMap(lambda x: x).collect()
bikepark_total =table_bikepark.select("nbike").rdd.flatMap(lambda x: x).collect()
bikepark_free =table_bikepark.select("nfree").rdd.flatMap(lambda x: x).collect()


map = dbc.Container(
    [
    dbc.Row(
          [
              dbc.Col(
                        html.Div(
                            html.H3(children='Parking Infrastructure Status'),
                        ),className="offset-by-five column"
                     )
          ]
      ),
    dbc.Row(
        dbc.Col(

            html.Div([
                daq.ToggleSwitch(
                    id='carbikeswitch',
                    #color="DarkBlue",
                    theme = "dark",
                    label=["Car","Bike"],
                    value=False
                ),

            ]
            ), className="offset-by-two column"

        )
             ),
    dbc.Row(
        [
            dbc.Col(
                        html.Div(
                            dcc.Graph(id="map-park"),
                        ),className="offset-by-one column six columns"#className="offset-by-one column"
                   ),
        ]
    )
            ],

    )
@app.callback(
    Output('map-park', 'figure'),
    [Input('carbikeswitch', 'value')])
def update_graph(value):

    if value == False:
        fig = go.Figure()
        fig.add_trace(go.Scattermapbox(
            lat=carpark_lat,
            lon=carpark_lon,
            mode='markers',
            marker=go.scattermapbox.Marker(
                size=12,
                #color='rgb(0, 0, 100)',
                # opacity=0.7,
                symbol='car'
            ),
            text=[f' <b>Name: {n} <br><br> Bay Type: Disabled    Pay and Display </b><br><br>'
                  f' Count:            {c}            {cp}<br>'
                  f' Occupied:       {o}            {oc}<br>'
                  f' Free:              {f}            {fr}<br>'

                  for n, c, o, f, cp, oc, fr in
                  list(zip(carpark_name, dis_count, dis_occupied, dis_free, pay_count, pay_occupied, pay_free))],

            hoverinfo='text'
        ))

        fig.update_layout(
            title={'text':'Car Parking Infrastructure',
                   'x':0.5,
                   'y':0.9,
                   'xanchor':'center',
                   'yanchor':'top'},
            autosize=True,
            width=1000,
            height=600,
            hovermode='closest',
            showlegend=False,
            mapbox=go.layout.Mapbox(
                accesstoken=mapbox_access_token,
                bearing=0,
                center=go.layout.mapbox.Center(
                    lat=51.517327,
                    lon=-0.120005
                ),
                pitch=0,
                zoom=9,
                style='light'
            ),
        )
        return fig

    else:
        fig = go.Figure()
        fig.add_trace(go.Scattermapbox(
            lat=bikepark_lat,
            lon=bikepark_lon,
            mode='markers',
            marker=go.scattermapbox.Marker(
                size=12,
                color='rgb(0, 0, 100)',
                # opacity=0.7,
                symbol='bicycle-share'
            ),
            text=[f'Name: {b_n} <br> Docks: {b_d} <br> Bikes: {b_b} <br> Free: {b_f}'
                  for b_n, b_d, b_b, b_f in
                  list(zip(bikepark_name, bikepark_docks, bikepark_total, bikepark_free))],

            hoverinfo='text'
        ))
        fig.update_layout(
            title={'text': 'Bike Parking Infrastructure',
                   'x': 0.5,
                   'y': 0.9,
                   'xanchor': 'center',
                   'yanchor': 'top'},
            autosize=True,
            width=1000,
            height=600,
            hovermode='closest',
            showlegend=False,
            mapbox=go.layout.Mapbox(
                accesstoken=mapbox_access_token,
                bearing=0,
                center=go.layout.mapbox.Center(
                    lat=51.517327,
                    lon=-0.120005
                ),
                pitch=0,
                zoom=9,
                style='light'
            ),
        )
        return fig




layout = html.Div([
        nav,
        map,
        ])
