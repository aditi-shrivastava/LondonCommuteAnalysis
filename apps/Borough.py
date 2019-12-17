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

table_sexage_14 = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
    .option("dbtable", "sexage_14") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "anuj_db") \
    .option("password", "anuj_db") \
    .load()

table_sexage_14.createOrReplaceTempView('table_sexage_2014')

table_sexage_15 = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
    .option("dbtable", "sexage_15") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "anuj_db") \
    .option("password", "anuj_db") \
    .load()

table_sexage_15.createOrReplaceTempView('table_sexage_2015')

table_sexage_16 = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
    .option("dbtable", "sexage_16") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "anuj_db") \
    .option("password", "anuj_db") \
    .load()

table_sexage_16.createOrReplaceTempView('table_sexage_2016')

table_sexage_17 = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
    .option("dbtable", "sexage_17") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "anuj_db") \
    .option("password", "anuj_db") \
    .load()

table_sexage_17.createOrReplaceTempView('table_sexage_2017')

table_sexage_18 = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
    .option("dbtable", "sexage_18") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "anuj_db") \
    .option("password", "anuj_db") \
    .load()

table_sexage_18.createOrReplaceTempView('table_sexage_2018')

sexage_borough_14 = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
    .option("dbtable", "report_borough_14") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "anuj_db") \
    .option("password", "anuj_db") \
    .load()

sexage_borough_14.createOrReplaceTempView('sexage_borough_2014')

sexage_borough_15 = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
    .option("dbtable", "report_borough_15") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "anuj_db") \
    .option("password", "anuj_db") \
    .load()

sexage_borough_15.createOrReplaceTempView('sexage_borough_2015')

sexage_borough_16 = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
    .option("dbtable", "report_borough_16") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "anuj_db") \
    .option("password", "anuj_db") \
    .load()

sexage_borough_16.createOrReplaceTempView('sexage_borough_2016')

sexage_borough_17 = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
    .option("dbtable", "report_borough_17") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "anuj_db") \
    .option("password", "anuj_db") \
    .load()

sexage_borough_17.createOrReplaceTempView('sexage_borough_2017')

sexage_borough_18 = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
    .option("dbtable", "report_borough_18") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "anuj_db") \
    .option("password", "anuj_db") \
    .load()

sexage_borough_18.createOrReplaceTempView('sexage_borough_2018')

borough_summary = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/anuj_db") \
    .option("dbtable", "borough_summary") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "anuj_db") \
    .option("password", "anuj_db") \
    .load()

borough_summary.createOrReplaceTempView('borough_summary')

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

dropdown = dbc.Container(
    [
        dcc.Tabs(id="year", value='2018', children=[
        #dcc.Tab(label='5 Year', value='5', style=tab_style, selected_style=tab_selected_style),
        dcc.Tab(label='2018', value='2018', style=tab_style, selected_style=tab_selected_style),
        dcc.Tab(label='2017', value='2017', style=tab_style, selected_style=tab_selected_style),
        dcc.Tab(label='2016', value='2016', style=tab_style, selected_style=tab_selected_style),
        dcc.Tab(label='2015', value='2015', style=tab_style, selected_style=tab_selected_style),
        dcc.Tab(label='2014', value='2014', style=tab_style, selected_style=tab_selected_style),
    ], style=tabs_styles),

      dbc.Row(
          [
              dbc.Col(
                        html.Div(
                            html.H3(children='People Killed or Seriously Injured in Boroughs'),
                        ),className="offset-by-three column"
                     )
          ]
      ),
       dbc.Row(
           [
               dbc.Col(
                  [
                     html.Div(
                (
                dcc.Dropdown(id='drop',
                options=[
                 {'label': 'RICHMOND-UPON-THAMES', 'value': 'RICHMOND-UPON-THAMES'},
                 {'label': 'CROYDON', 'value': 'CROYDON'},
                 {'label': 'TOWER HAMLETS', 'value': 'TOWER HAMLETS'},
                 {'label': 'GREENWICH', 'value': 'GREENWICH'},
                 {'label': 'CAMDEN', 'value': 'CAMDEN'},
                 {'label': 'HAMMERSMITH & FULHAM', 'value': 'HAMMERSMITH & FULHAM'},
                 {'label': 'ENFIELD', 'value': 'ENFIELD'},
                 {'label': 'HOUNSLOW', 'value': 'HOUNSLOW'},
                 {'label': 'BEXLEY', 'value': 'BEXLEY'},
                 {'label': 'LEWISHAM', 'value': 'LEWISHAM'},
                 {'label': 'REDBRIDGE', 'value': 'REDBRIDGE'},
                 {'label': 'CITY OF LONDON', 'value': 'CITY OF LONDON'},
                 {'label': 'LAMBETH', 'value': 'LAMBETH'},
                 {'label': 'HARROW', 'value': 'HARROW'},
                 {'label': 'WESTMINSTER', 'value': 'WESTMINSTER'},
                 {'label': 'ISLINGTON', 'value': 'ISLINGTON'},
                 {'label': 'KINGSTON-UPON-THAMES', 'value': 'KINGSTON-UPON-THAMES'},
                 {'label': 'BROMLEY', 'value': 'BROMLEY'},
                 {'label': 'KENSINGTON & CHELSEA', 'value': 'KENSINGTON & CHELSEA'},
                 {'label': 'SOUTHWARK', 'value': 'SOUTHWARK'},
                 {'label': 'HARINGEY', 'value': 'HARINGEY'},
                 {'label': 'WALTHAM FOREST', 'value': 'WALTHAM FOREST'},
                 {'label': 'HILLINGDON', 'value': 'HILLINGDON'},
                 {'label': 'SUTTON', 'value': 'SUTTON'},
                 {'label': 'HAVERING', 'value': 'HAVERING'},
                 {'label': 'MERTON', 'value': 'MERTON'},
                 {'label': 'BRENT', 'value': 'BRENT'},
                 {'label': 'BARNET', 'value': 'BARNET'},
                 {'label': 'WANDSWORTH', 'value': 'WANDSWORTH'},
                 {'label': 'NEWHAM', 'value': 'NEWHAM'},
                 {'label': 'HACKNEY', 'value': 'HACKNEY'},
                 {'label' : 'BARKING & DAGENHAM', 'value':'BARKING & DAGENHAM'},
                    {'label' : 'EALING','value':'EALING'}

             ],
             value='CITY OF LONDON'  
             )
                ),className="offset-by-four column six columns"
                             ),

                   ],

               )
            ]
       ),
        dbc.Row(
            [
                dbc.Col(
                    html.Div(

                        html.H2(id="killed",children=''),
                        ),style={'color':'DarkBlue'}, className="offset-by-three column",

                ),
                dbc.Col(
                    html.Div(
                        html.H2(id="injured",children=''),
                    ),style={'color':'DarkBlue'}, className="offset-by-one column"
                )

            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    html.Div(
                        html.H6(children='People were killed'),
                    ), className="offset-by-three column"
                ),
                dbc.Col(
                    html.Div(
                        html.H6(children='People were seriously injured'),
                    ), className="offset-by-one column"
                )

            ]
        ),
        dbc.Row(
            [
              dbc.Col(
                 [
                  html.Div(
                  dcc.Graph(id='app-2-display-value'),
                   className="offset-by-one column twelve columns"
                )

                        ]
                  ),
               dbc.Col(
                   [
                       html.Div(
                           dcc.Graph(id='graph2'),
                            className="offset-by-one column twelve columns"
                       )
                   ]
               )

                ]
            )
       ],
    )

@app.callback(
    Output('app-2-display-value', 'figure'),
    [Input('drop', 'value'),
    Input('year', 'value')])
def update_graph(drop,year):

    report_borough = spark.sql(
        "SELECT num,modeoftravel FROM borough_summary WHERE "
        "year =" + '"{}"'.format(year) + " AND borough =" + '"{}"'.format(drop))

    ncas = report_borough.select("num").rdd.flatMap(lambda x: x).collect()
    lab = report_borough.select("modeoftravel").rdd.flatMap(lambda x: x).collect()
    print(ncas)
    print(lab)
    data = [
        {
            'values': ncas,
            'labels': lab,
            'type': 'pie',
            'hole': 0.2
        },
    ]
    return {
                 'data': data,
                 'layout': {'title': 'Mode of Travel: ' + drop}
            }

@app.callback(
    Output('graph2', 'figure'),
    [Input('drop', 'value'),
     Input('year', 'value')])
def update_graph(drop, year):
    ans = spark.sql(
        "SELECT age,csex,count FROM sexage_borough_"+year+" WHERE borough  =" + '"{}"'.format(drop) +" ORDER BY age")

    yaxismale = ans.filter(ans['csex'] == 'Male').select(ans['count'])
    yaxism = yaxismale.select("count").rdd.flatMap(lambda x: x).collect()
    yaxisfemale = ans.filter(ans['csex'] == 'Female').select(ans['count'])
    yaxisf = yaxisfemale.select("count").rdd.flatMap(lambda x: x).collect()

    fig = go.Figure(data=[
        go.Bar(name='Male', x=['0-15', '16-24', '25-59', '60+'], y=yaxism),
        go.Bar(name='Female', x=['0-15', '16-24', '25-59', '60+'], y=yaxisf),

    ])

    trace1 = go.Bar(
        x=['0-15', '16-24', '25-59', '60+'], y=yaxism,
        name='Male'
    )
    trace2 = go.Bar(
        x=['0-15', '16-24', '25-59', '60+'], y=yaxisf,
        name='Female'
    )

    data = [trace1, trace2]
    return {
                 'data': data,
                 'layout': {'title': 'Age and Gender: ' + drop,'barmode':'stack',
                            'xaxis':{'title':'Age'},
                            'yaxis':{'title':'Count'}}
            }

@app.callback(
    Output('killed', 'children'),
    [Input('drop', 'value'),
     Input('year', 'value')])
def update_graph(drop, year):
     query_killed = spark.sql("SELECT count(1) from table_sexage_"+year+" WHERE cseverity = 'Fatal' AND borough  =" + '"{}"'.format(drop))
     value_killed = query_killed.rdd.flatMap(lambda x: x).collect()
     return value_killed

@app.callback(
    Output('injured', 'children'),
    [Input('drop', 'value'),
     Input('year', 'value')])
def update_graph(drop, year):
     query_injured =spark.sql("SELECT count(1) from table_sexage_"+year+" WHERE cseverity = 'Serious' AND borough  =" + '"{}"'.format(drop))
     value_injured = query_injured.rdd.flatMap(lambda x: x).collect()
     return value_injured


layout = html.Div([
        nav,
        dropdown,
        ])