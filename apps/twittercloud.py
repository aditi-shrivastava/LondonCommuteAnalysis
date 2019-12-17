import dash_html_components as html
import dash_daq as daq
import dash_bootstrap_components as dbc
from dash.dependencies import Output, Input
import base64
import matplotlib.pyplot as plt
import re
from twython import Twython
from wordcloud import WordCloud, STOPWORDS
from app import app
from navbar import Navbar
app.config['suppress_callback_exceptions']=True
nav = Navbar()

APP_KEY = "D4P9kxxnr4IQWXcXabrziMV18"
APP_SECRET = "rqmQLjoHgohPG1Ts819vxlpE6wnSr2xQsJpgr9gAqLqkbn7qfZ"
twitter = Twython(APP_KEY, APP_SECRET)

cloud = dbc.Container(
    [
        html.Div([

            html.Img(id='image',src='wordcloud.png',height=550,width=700)
            ],className='offset-by-three column'),
            html.Div(

                daq.Slider(
                    id='image-dropdown',
                    min=100,
                    max=2000,
                    size=700,
                    value=400,
                    handleLabel={"showCurrentValue": True, "label": "Tweets"},
                    marks={str(year): str(year) for year in (100, 400, 800, 1200, 1600, 2000)},
                    step=None
                ), className="offset-by-three column six columns"
            ),

       ],
#className="mt-4",
)

@app.callback(
    Output('image', 'src'),
    [Input('image-dropdown', 'value')])
def update_image_src(maxtweet):
    #Get timeline
    user_timeline=twitter.get_user_timeline(screen_name='Tfl',count=1)
    #get most recent id
    last_id = user_timeline[0]['id']-1
    for i in range(16):
        batch = twitter.get_user_timeline(screen_name='Tfl',count=200, max_id=last_id)
        user_timeline.extend(batch)
        last_id = user_timeline[-1]['id'] - 1

    raw_tweets = []
    for tweets in user_timeline:
        raw_tweets.append(tweets['text'])

    #print(raw_tweets)

    raw_string = ''.join(raw_tweets)

    no_links = re.sub(r'http\S+', '', raw_string)
    no_unicode = re.sub(r"\\[a-z][a-z]?[0-9]+", '', no_links)
    no_special_characters = re.sub('[^A-Za-z ]+', '', no_unicode)

    words = no_special_characters.split(" ")
    words = [w for w in words if len(w) > 2]  # ignore a, an, be, ...
    words = [w.lower() for w in words]
    words = [w for w in words if w not in STOPWORDS]

    wc = WordCloud(collocations=False,scale=4,repeat=True,background_color="white", max_words=maxtweet,max_font_size=50,width=200,height=150,prefer_horizontal=.9,relative_scaling=1)
    clean_string = ','.join(words)
    abc=wc.generate(clean_string)

    plt.figure(frameon=False)
    plt.imshow(wc, interpolation='bilinear')
    plt.axis("off")
    plt.tight_layout()
    plt.savefig("wordcloud.png", format="png")
    encoded_image = base64.b64encode(open("wordcloud.png", 'rb').read())
    return 'data:image/png;base64,{}'.format(encoded_image.decode())

layout = html.Div([
        nav,
        cloud,
        ])