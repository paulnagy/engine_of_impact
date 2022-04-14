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
        data.append({'pubmedID':item['data']['pubmedID'],
                    'creationDate':item['data']['creationDate'],
                    'citations':citation_count,
                    'fullAuthor':item['data']['fullAuthor'],
                    'title':item['data']['title'],
                    'journalTitle':item['data']['journalTitle'],
                    'pubYear':item['data']['pubYear']})
    df1=pd.DataFrame(data)     
    df1['authors']=""
    for i,row in df1.iterrows():
        authors=ast.literal_eval(row['fullAuthor'])
        if len(authors)>2:
            auth_list="{}, + {} co-authors,  {}".format(authors[0].replace(',',''),len(authors)-2,authors[-1].replace(',',''))
        elif len(authors)>0:
            auth_list=authors[0].replace(',','')
        df1.loc[i,'authors']=auth_list
    df1['pubDate']=df1.creationDate.str[:-6]
    df2=df1.groupby('pubYear').pubmedID.count().reset_index()
    df2.columns=['Year','Count']
    bar_fig=px.bar(
        data_frame=df2,
        x="Year",
        y='Count',
        title="OHDSI Publications")
    df3=df1.groupby('pubYear').citations.sum().reset_index()
    df3['cumulative']=df3.citations.cumsum()
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
    df1['publication']=df1.apply(lambda row:"[{}](http://pubmed.gov/{})".format(row.title,row.pubmedID),axis=1)
    cols=['pubDate','authors','publication','journalTitle','citations']
    layout=html.Div(
        children=[
                dcc.Graph(id='publications',figure=fig), 
                html.Div(),
                dash_table.DataTable(df1.sort_values('pubDate',ascending=False).to_dict('records'), 
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
                        page_size=20,
                        page_action='native',
                        filter_action='native'

                     )
        ]
    )
    return layout