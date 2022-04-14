import dash
import ast
from dash import dcc, html, dash_table
from api_miners import key_vault, pubmed
from views import pubs
import plotly.express as px
import pandas as pd 

app=dash.Dash()
app.layout= pubs.build_pubs_dash()

if __name__ == '__main__':
    app.run_server(debug=True)
