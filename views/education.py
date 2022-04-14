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
    df4=df1.groupby('pubYear').pubmedID.count().reset_index()
    df4.columns=['Year','Count']
    bar_fig=px.bar(
        data_frame=df4,
        x="Year",
        y='Count',
        title="OHDSI Publications")
    df4=df1.groupby('pubYear').citations.sum().reset_index()
    df4['cumulative']=df4.citations.cumsum()
    df4.columns=['Year','citations','Count']
    line_fig=px.line(
        data_frame=df4,
        x="Year",
        y='Count',
        title="OHDSI Cumulative Citations")
    df1['authors']=""
    for i,row in df1.iterrows():
        authors=ast.literal_eval(row['fullAuthor'])
        if len(authors)>2:
            auth_list="{}, + {} co-authors,  {}".format(authors[0].replace(',',''),len(authors)-2,authors[-1].replace(',',''))
        elif len(authors)>0:
            auth_list=authors[0].replace(',','')
        df1.loc[i,'authors']=auth_list
    df1['pubDate']=df1.creationDate.str[:-6]
    df1.drop(['fullAuthor','pubYear','creationDate'],axis=1,inplace=True)
    cols=['pubmedID','pubDate','citations','authors','title','journalTitle']
    layout=html.Div(
        children=[
                dcc.Graph(id='publications',figure=bar_fig),
                dcc.Graph(id='citations',figure=line_fig),  
                html.Div(),
                dash_table.DataTable(df1.sort_values('pubDate',ascending=False).to_dict('records'), 
                        [{"name": i, "id": i} for i in cols],
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
                        page_action='native'
                     )
        ]
    )
    return layout