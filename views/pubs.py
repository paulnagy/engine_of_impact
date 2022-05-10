import dash
import ast
from dash import dcc, html, dash_table
from api_miners import key_vault, pubmed
import plotly.express as px
import pandas as pd 

def build_pubs_dash():
    container_name='pubmed'
    key_dict = key_vault.get_key_dict()
    container=pubmed.init_cosmos(key_dict, container_name)
    query = "SELECT * FROM c"
    items = list(container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))
    data=[]
    for item in items:
        t=0
        for citations in item['data']['trackingChanges']:
            if citations['t']>t:
                t=citations['t']
                citation_count=citations['numCitations']
        data.append({'PubMed ID':item['data']['pubmedID'],
                    'Creation Date':item['data']['creationDate'],
                    'Citation Count':citation_count,
                    'Authors':item['data']['fullAuthor'],
                    'Title':item['data']['title'],
                    'Journal':item['data']['journalTitle'],
                    'Publication Year':item['data']['pubYear'],
                    'MeSH Terms':item['data']['meshT']})
    df1=pd.DataFrame(data)   

    #parse authors to set a limit on authors shown n_authors
    df1['authors']=""
    n_authors=3
    for i,row in df1.iterrows():
        authors=ast.literal_eval(row['Authors'])
        auth_list=""
        if len(authors)>n_authors:
            for j in range(n_authors):
                auth_list+="{}, ".format(authors[j].replace(',',''))
            auth_list += "+ {} authors, ".format(len(authors)-n_authors)
            auth_list += "{} ".format(authors[-1].replace(',',''))
        else:
            for auth in authors:
                auth_list+="{}, ".format(auth.replace(',',''))
            auth_list=auth_list[:-2]
        df1.loc[i,'Authors']=auth_list

    df1['Publication Date']=df1['Creation Date'].str[:-6]
    df2=df1.groupby('Publication Year')['PubMed ID'].count().reset_index()
    df2.columns=['Year','Count']
    bar_fig=px.bar(
        data_frame=df2,
        x="Year",
        y='Count',
        title="OHDSI Publications")
    df3=df1.groupby('Publication Year')['Citation Count'].sum().reset_index()
    df3['cumulative']=df3['Citation Count'].cumsum()
    df3.columns=['Year','citations','Count']
    line_fig=px.line(
        data_frame=df3,
        x='Year',
        y='Count',
        title="OHDSI Cumulative Citations")
        
    from plotly.subplots import make_subplots
    import plotly.graph_objects as go
    fig = make_subplots(rows=1, cols=2,
                        subplot_titles=("Publications","Cumulative Citations"))
    fig.add_trace(
        go.Bar(
        x=df2['Year'],
        y=df2['Count']),
        row=1, col=1
        )
    fig.add_trace(
        go.Line(
        x=df3['Year'],
        y=df3['Count']),
        row=1, col=2
        )
    fig.update_layout( title_text="Publications Analysis", showlegend=False)
    df1['Publication']=df1.apply(lambda row:"[{}](http://pubmed.gov/{})".format(row.Title,row['PubMed ID']),axis=1)
    cols=['Publication Date','Authors','Publication','Journal','Citation Count']
    layout= html.Div([
                dcc.Interval(
                    id='interval-component',
                    interval=1*1000 # in milliseconds
                ),
                html.Div(
                    children=[
                            html.Div(dcc.Input(id='input-on-submit', type='text', value = "")),
                            html.Button('Add Article', id='submit-val'),
                            html.Div(id='container-button-basic',
                                    children='Enter article PubMed ID or name'),
                            
                            dcc.Graph(id='publications',figure=fig), 
                            html.Div(id='my-output'),
                            dash_table.DataTable(df1.sort_values('Publication Date',ascending=False).to_dict('records'), 
                                    [{"name": i, "id": i,'presentation':'markdown'} for i in cols],
                                    style_cell={
                                        'height': 'auto',
                                        # all three widths are needed
                                        'minWidth': '10px', 'width': '10px', 'maxWidth': '250px',
                                        'whiteSpace': 'normal',
                                        'textAlign': 'left'
                                    },
                                    sort_action='native',
                                    page_current=0,
                                    page_size=10,
                                    page_action='native',
                                    filter_action='native',
                                    style_data={
                                        'color': 'black',
                                        'backgroundColor': 'white',
                                        'font-family': 'Saira Extra Condensed'
                                    },
                                    style_filter=[
                                        {
                                            'color': 'black',
                                            'backgroundColor': 'white',
                                            'font-family': 'Saira Extra Condensed'
                                        }
                                    ],
                                    style_header={
                                        'font-family': 'Saira Extra Condensed',
                                        'color': 'black',
                                        'fontWeight': 'bold'
                                    }
                                )
                            
                    ]
                )
            ])
    return layout

