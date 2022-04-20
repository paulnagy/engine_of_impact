import dash
import ast
from dash import dcc, html, dash_table
from api_miners import key_vault, pubmed, youtube
from views import pubs,education
import plotly.express as px
import pandas as pd 

#youtube.main()
pubmed.main()
app=dash.Dash()
app.layout= pubs.build_pubs_dash()
#app.layout= education.build_education_dash()

if __name__ == '__main__':
    app.run_server(debug=True)
