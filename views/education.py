import dash
import ast
from dash import dcc, html, dash_table
from api_miners import key_vault, pubmed
import plotly.express as px
import pandas as pd 

def convert_time(time_str):
    """Takes time values from Youtube duration
        '8M12S' or '3H10M5S' 
    """
    import datetime,time
    #Strip PT from string (Period Time)
    time_str=time_str[2:]
    filter=''
    filter_list=['H','M','S']
    for filter_item in filter_list:
        if filter_item in time_str:
            filter+='%'+filter_item*2
    ntime=time.strptime(time_str,filter)
    return datetime.timedelta(hours=ntime.tm_hour,minutes=ntime.tm_min,seconds=ntime.tm_sec)


def build_education_dash():
    container_name='youtube'
    key_dict = key_vault.get_key_dict()
    container=pubmed.init_cosmos(key_dict, container_name)
    query = "SELECT * FROM c"
    items = list(container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))
    videos=[]
    for item in items:
        #Review the log of counts and find the last two and subtract them for recent views
        df=pd.DataFrame(item['counts']).sort_values('checkedOn',ascending=False).reset_index()
        total_views=int(df.viewCount[0])
        if len(df)==1:
            recent_views=int(df.viewCount[0])
        else:
            recent_views=int(df.viewCount[0])-int(df.viewCount[1])
        videos.append({'id':item['id'],
                    'title':item['title'],
                    'duration':convert_time(item['duration']),
                    'pubDate':pd.to_datetime(item['publishedAt']),
                    'totalViews':total_views,
                    'recentViews':recent_views,
                    'channelTitle':item['channelTitle']}
                    )
    df=pd.DataFrame(videos)
    import plotly.express as px
    df=df[df.channelTitle.str.startswith('OHDSI')].copy(deep=True)
    df['yr']=df['pubDate'].dt.year


    from plotly.subplots import make_subplots
    import plotly.graph_objects as go
    fig = make_subplots(rows=1, cols=2,
                        subplot_titles=("Youtube Hours Created","Cumulative Hrs Watched"))
    df4=df.groupby('yr').duration.sum().reset_index()
    df4.columns=['Year','SumSeconds']
    df4['Hrs Created']=df4['SumSeconds'].dt.days*24+df4['SumSeconds'].dt.seconds/3600
    fig.add_trace(
        go.Bar(
        x=df4['Year'],
        y=df4['Hrs Created']),
        row=1, col=1
        )
    df['hrsWatched']=(df.duration.dt.days*24+df.duration.dt.seconds/3600)*df.totalViews
    df4=df.groupby('yr').hrsWatched.sum().reset_index()
    df4.columns=['Year','HrsWatched']
    df4['Cumulative Hrs Watched']=df4['HrsWatched'].cumsum()
    fig.add_trace(
        go.Line(
        x=df4['Year'],
        y=df4['Cumulative Hrs Watched']),
        row=1, col=2
        )
    df['pubDate']=df.pubDate.dt.strftime('%Y-%m-%d')
    df['title']=df.apply(lambda row:"[{}](https://www.youtube.com/watch?v={})".format(row.title,row.id),axis=1)
    fig.update_layout( title_text="Youtube Video Analysis", showlegend=False)
    cols=['title','pubDate','duration','totalViews','recentViews']
    layout=html.Div(
        children=[
                dcc.Graph(id='videos',figure=fig), 
                html.Div(),
                dash_table.DataTable(df.sort_values('pubDate',ascending=False).to_dict('records'), 
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