import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from app import app
from apps import Borough,park,stationusage,accidentmap,boroughload,twittercloud,twitter,accidentfactor,cycleusage,bus_usage
import homepage


def toggle_collapse(n, is_open):
    if n:
        return not is_open
    return is_open

def set_navitem_class(is_open):
    if is_open:
        return "open"
    return ""

for i in [1, 2,3,4,5]:
    app.callback(
        Output(f"submenu-{i}-collapse", "is_open"),
        [Input(f"submenu-{i}", "n_clicks")],
        [State(f"submenu-{i}-collapse", "is_open")],
    )(toggle_collapse)

    app.callback(
        Output(f"submenu-{i}", "className"),
        [Input(f"submenu-{i}-collapse", "is_open")],
    )(set_navitem_class)

CONTENT_STYLE = {
    "font-family" : "Arial"
}
app.layout = html.Div([
    dcc.Location(id = 'url', refresh = False),
    html.Div(id = 'page-content',style=CONTENT_STYLE)
])
app.config['suppress_callback_exceptions']=True

@app.callback(Output('page-content', 'children'),
            [Input('url', 'pathname')])
def display_page(pathname):

    if pathname == '/apps/t_sentiment':
        return twitter.layout
    elif pathname == '/apps/t_cloud':
        return twittercloud.layout
    elif pathname == '/apps/a_borough':
        return Borough.layout
    elif pathname =='/apps/a_accidentfactors':
        return accidentfactor.layout
    elif pathname =='/apps/a_park':
        return park.layout
    elif pathname =='/apps/a_cycleusage':
        return cycleusage.layout
    elif pathname == '/apps/a_boroughload':
        return boroughload.layout
    elif pathname =='/apps/a_stationusage':
        return stationusage.layout
    elif pathname =='/apps/a_accidentmap':
        return accidentmap.layout
    elif pathname == '/apps/perf':
        return bus_usage.layout
    elif pathname == '/home':
        return homepage.layout
    else:
        return homepage.layout

if __name__ == '__main__':
    app.run_server(debug=True)