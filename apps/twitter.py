import dash_core_components as dcc
import dash_html_components as html
import dash_daq as daq
import dash_bootstrap_components as dbc
from dash.dependencies import Output, Input
from twython import Twython
from navbar import Navbar
from textblob import TextBlob
import sys,tweepy
from app import app
app.config['suppress_callback_exceptions']=True
nav = Navbar()

consumerkey = "Kzvz0U6BnEe2agvFrYJ5Y7QWB"
consumersecret = "fPtrQdzjzqpO0sL3MEyrtP6qhB1QPgOqRjgzAKv5TVVVCVhQE3"
accesstoken = "834530227-CTHfzXJ2pDhYuNqHSdN7wQdCeIaPwD3wnHd5pUfW"
accesssecret = "OEWbAA2mzFqVjALczMvrICYrceDGXcThtc1kAq27UYYOM"
twitter = Twython(consumerkey, consumersecret)
auth = tweepy.OAuthHandler(consumer_key=consumerkey,consumer_secret=consumersecret)
auth.set_access_token(accesstoken,accesssecret)
api = tweepy.API(auth)
searchterm = 'tfl'
public_tweets = api.search(searchterm,count=100)

dropdown = dbc.Container(
    [
      dbc.Row(
          [
              dbc.Col(
                        html.Div(
                            html.H2(children='LIVE Analysis for TFL'),
                        ),className="offset-by-four column"
                     )
          ]
      ),
       dbc.Row(
           [
               dbc.Col(
                  [
                     html.Div(
                (
                dcc.Graph(id = 'tfl_sentiment')
                ),className="offset-by-one column twelve columns"
                             ),

                   ],

               ),
            ]
       ),
        dbc.Row(
            [
              dbc.Col(
                 [
                  html.Div(
                  daq.Slider(
                        id='tweet-slider',
                        min=200,
                        max=2000,
                        size = 600,
                        value=400,
                        handleLabel={"showCurrentValue": True,"label": "Tweets"},
                        marks={str(year): str(year) for year in (200,400,600,800,1000,1500,2000)},
                        step=None
                      ), className="offset-by-three column six columns"
                )

                        ]
                  ),
                ]
            )
       ],
)

@app.callback(
    Output('tfl_sentiment', 'figure'),
    [Input('tweet-slider', 'value')])
def update_figure(maxtweet):
    positive = 0
    negative = 0
    neutral = 0
    polarity = 0
    count = 0
    for tweet in tweepy.Cursor(api.search,q=searchterm+" -filter:retweets",count=100,include_entities=True,lang="en",).items(maxtweet):
        analysis = TextBlob(tweet.text)
        if analysis.sentiment.polarity > 0.0:
            positive = positive + 1
        elif analysis.sentiment[0] < 0.0:
            negative = negative + 1
        elif analysis.sentiment[0] == 0.0:
            neutral = neutral + 1

        count = count + 1

    labels = ['Positive','Negative','Neutral']
    values = [positive,negative,neutral]

    data = [
        {
            'values': values,
            'labels': labels,
            'type': 'pie',
            'hole': 0.2
        },
    ]

    return {
        'data': data,
        'layout': {'title': str(count) + ' tweets'}

    }


layout = html.Div([
        nav,
        dropdown,
        ])


