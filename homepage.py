import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from navbar import Navbar
from app import app

nav = Navbar()
app.config['suppress_callback_exceptions']=True
card_content_1 = [
    dbc.CardBody(
        [
        html.P(
                "City of Westminster remains to be the most congested borough from 2013-2017",
                className="card-text",
            ),
        ]
    ),
]

card_content_2 = [
    dbc.CardBody(
        [
            #html.H1("83%", className="card-title"),
            html.P(
                "From 2013 to 2017, there has been an increase in 6.29 million people using Tube in the City of Westminster",
                className="card-text",
            ),
        ]
    ),
]

card_content_3 = [
    dbc.CardBody(
        [
            html.P("The highest increase in the use of Tube per borough which is 14.89% (or 31.41 million people) was observed in the City of London",
                className="card-text",
            ),
            #html.H1("FRIDAY", className="card-title"),

        ]
    ),
]

card_content_4 = [
    dbc.CardBody(
        [
            #html.H1("83%", className="card-title"),
            html.P(
                "Waterloo station in Lambeth remains to be the most populated station from 2013 to 2017 (except 2014 when Oxford Circus in City of Westminster tops the list)",
                className="card-text",
            ),
        ]
    ),
]

card_content_5 = [
    dbc.CardBody(
        [
            html.P(
                "People are more likely to use Tube on weekdays compared to weekends, Sunday being the least crowded day.",
                className="card-text",
            ),

        ]
    ),
]

card_content_6 = [
    dbc.CardBody(
        [
            html.P(
                "Most number of accidents occur on",
                className="card-text",
            ),
            html.H1("FRIDAY")

        ]
    ),
]

card_content_7 = [

    dbc.CardBody(
        [

            html.P(
                "Westminster and Lambeth account for almost 11% of the accidents every year",
                className="card-text",
            ),

        ]
    ),
]

card_content_8 = [
    dbc.CardBody(
        [
            html.H1("83%", className="card-title"),
            html.P(
                "accidents occur in Fine weather condition",
                className="card-text",
            ),
        ]
    ),
]

card_content_9 = [
    dbc.CardBody(
        [
            html.H1("87%", className="card-title"),
            html.P(
                "increase in Fatal/Serious accidents from 2014 to 2018",
                className="card-text",
            ),
        ]
    ),
]

card_content_10 = [
    dbc.CardBody(
        [
            html.H2("Upto 75%", className="card-title"),
            html.P(
                "increase in Cycle Usage in June and July every year",
                className="card-text",
            ),
        ]
    ),
]


card_content_11 = [

    dbc.CardBody(
        [

            html.P(
                "Cyclists have travelled maximum distance between Hyde Park Corner -Albert Gate stations in last 5 years",
                className="card-text",
            ),

        ]
    ),
]

card_content_12 = [

    dbc.CardBody(
        [

            html.P(
                "Lansdowne Road station has witnessed maximum cycle usage in last 7 years",
                className="card-text",
            ),

        ]
    ),
]

card_content_13 = [

    dbc.CardBody(
        [

            html.P(
                "Maximum number of accidents occur between 16:00 to 20:00",
                className="card-text",
            ),

        ]
    ),
]

body = dbc.Container(
    [
        html.Div(
            [
            dbc.CardColumns(
            [
                dbc.Card(card_content_1, color="info", inverse=True, style={"width": "30rem", "height": "10rem"}),
                dbc.Card(card_content_2, color="success", inverse=True, style={"width": "30rem", "height": "10rem"}),
                dbc.Card(card_content_6, color="danger", inverse=True, style={"width": "30rem", "height": "12rem"}),
                dbc.Card(card_content_10, color="success", inverse=True, style={"width": "30rem", "height": "16rem"}),
                dbc.Card(card_content_7, color="danger", inverse=True, style={"width": "30rem", "height": "10rem"}),
                dbc.Card(card_content_5, color="info", inverse=True, style={"width": "30rem", "height": "10rem"}),
                dbc.Card(card_content_12, color="success", inverse=True, style={"width": "30rem", "height": "7rem"}),
                dbc.Card(card_content_8, color="danger", inverse=True, style={"width": "30rem", "height": "13rem"}),
                dbc.Card(card_content_3, color="success", inverse=True, style={"width": "30rem", "height": "14rem"}),
                dbc.Card(card_content_11, color="success", inverse=True, style={"width": "30rem", "height": "10rem"}),
                dbc.Card(card_content_9, color="danger", inverse=True, style={"width": "30rem", "height": "13rem"}),
                dbc.Card(card_content_4, color="info", inverse=True, style={"width": "30rem", "height": "14rem"}),
                dbc.Card(card_content_13, color="danger", inverse=True, style={"width": "30rem", "height": "10rem"}),
            ]
        )
            ],className="offset-by-one column"
        )

       ],

)

layout = html.Div([nav,body])
