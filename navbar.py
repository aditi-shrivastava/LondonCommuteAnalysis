import dash_bootstrap_components as dbc
import dash_html_components as html

FA = "https://use.fontawesome.com/releases/v5.8.1/css/all.css"

SIDEBAR_STYLE = {
    "position": "fixed",
    "top": 0,
    "left": 0,
    "bottom": 0,
    "width": "18rem",
    "padding": "2rem 1rem",
    "background-color": "#4c4c57",
    "color": "#f4f5f7",
    "font-family" : "Arial"
}

submenu_1 = [
    html.Li(
        # use Row and Col components to position the chevrons
        dbc.Row(    #Screen is split in rows and columns. This is the first row with 2 columns
            [
                dbc.Col(html.I(className="fab fa-twitter"),width="2"),
                dbc.Col("Twitter",width="6"),
                dbc.Col(
                    html.I(className="fas fa-chevron-down mr-2") #-down was -right. mr-X, X is the position
                ),

            ],
            no_gutters=True,
            className="my-1",
        ),
        id="submenu-1",
    ),

    dbc.Collapse(
        [
            dbc.NavLink("Sentiment Analysis", href="/apps/t_sentiment"),
            dbc.NavLink("Word Cloud", href="/apps/t_cloud"),
        ],
        id="submenu-1-collapse",
    ),
]

submenu_2 = [
    html.Li(
        dbc.Row(
            [
                dbc.Col(html.I(className="fas fa-car-crash"),width="2"),
                dbc.Col("Accidents",width="6"),
                dbc.Col(
                    html.I(className="fas fa-chevron-down mr-2"),
                ),
            ],
            no_gutters=True,
            className="my-1",
        ),
        id="submenu-2",
    ),
    dbc.Collapse(
        [
            dbc.NavLink("Borough Report", href="/apps/a_borough"),
            dbc.NavLink("Accident Locations", href="/apps/a_accidentmap"),
            dbc.NavLink("Accident Factors", href="/apps/a_accidentfactors")
        ],
        id="submenu-2-collapse",
    ),
]

submenu_3 = [
    html.Li(
        # use Row and Col components to position the chevrons
        dbc.Row(    #Screen is split in rows and columns. This is the first row with 2 columns
            [
                dbc.Col(html.I(className="fas fa-subway"),width="2"),
                dbc.Col("Tube",width="6"),
                dbc.Col(
                    html.I(className="fas fa-chevron-down mr-2") #-down was -right. mr-X, X is the position
                ),

            ],
            no_gutters=True,
            className="my-1",
        ),
        id="submenu-3",
    ),

    dbc.Collapse(
        [
            dbc.NavLink("Borough Distribution", href="/apps/a_boroughload"),
            dbc.NavLink("Station Congestion", href="/apps/a_stationusage"),
        ],
        id="submenu-3-collapse",
    ),
]

submenu_4 = [
    html.Li(
        dbc.Row(
            [
                dbc.Col(html.I(className="far fa-chart-bar"),width="2"),
                dbc.Col("Facilities",width="6"),
                dbc.Col(
                    html.I(className="fas fa-chevron-down mr-2"),
                ),
            ],
            no_gutters=True,
            className="my-1",
        ),
        id="submenu-4",
    ),
    dbc.Collapse(
        [

            dbc.NavLink("Cycle Usage", href="/apps/a_cycleusage"),
            dbc.NavLink("Parking", href="/apps/a_park"),



        ],
        id="submenu-4-collapse",
    ),
]

submenu_5 = [
    html.Li(
        dbc.Row(
            [
                dbc.Col(html.I(className="fas fa-bus-alt"),width="2"),
                dbc.Col("Bus",width="6"),
                dbc.Col(
                    html.I(className="fas fa-chevron-down mr-2"),
                ),
            ],
            no_gutters=True,
            className="my-1",
        ),
        id="submenu-5",
    ),
    dbc.Collapse(
        [
            dbc.NavLink("Performance", href="/apps/perf"),
        ],
        id="submenu-5-collapse",
    ),
]
def Navbar():
    navbar = html.Div(
        [
            html.A(
                [
                    html.Img(src='/assets/n9hyiWD.png',
                             style={
                                 'height': '20%',
                                 'width': '100%',
                                 'border-radius': '50%',
                                 'position': 'relative',
                                 'padding-top': 0,
                                 'padding-right': 0
                             }
                             ),
                ],href='/home'
            ),

            html.Hr(),
            html.P(
                "", className="lead"
            ),
            dbc.Nav(submenu_2 + submenu_3 + submenu_5 + submenu_4 + submenu_1, vertical=True),
        ],
        style=SIDEBAR_STYLE,
        id="sidebar",
    )
    return navbar