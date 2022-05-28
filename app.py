import datetime as date  
import pandas as pd 
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import dash
from dash import Dash, dcc, html, Input, Output, State
from flask import Flask
from flask_session import Session
from flask import Flask, current_app, flash, jsonify, make_response, redirect, request, render_template, send_file, Blueprint, url_for, redirect
from functools import wraps
from distutils.log import error

from azure.cosmos import CosmosClient, PartitionKey
from googleapiclient.discovery import build
import logging
from ms_identity_web import IdentityWebPython
from ms_identity_web.adapters import FlaskContextAdapter
from ms_identity_web.configuration import AADConfig

from api_miners import key_vault, youtube, pubmed
from api_miners.pubmed import *
from views import pubs, education
from dash.dash_table.Format import Format, Group
#App Configurations
app = Flask(__name__)
Session(app) # init the serverside session for the app: this is requireddue to large cookie size
aad_configuration = AADConfig.parse_json('aadconfig.json')
SESSION_TYPE = "filesystem"
SESSION_STATE = None
key_dict = key_vault.get_key_dict()
endpoint = key_dict['AZURE_ENDPOINT']
azure_key = key_dict['AZURE_KEY']
secret_api_key = key_dict['SERPAPI_KEY']


#CosmosDB Connection
client = CosmosClient(endpoint, azure_key)
database_name = 'ohdsi-impact-engine'
container = pubmed.init_cosmos(key_dict, 'pubmed')
container_ignore = pubmed.init_cosmos(key_dict, 'pubmed_ignore')

#Azure Authentication Configurations
secure_client_credential=None
app.logger.level=logging.INFO # can set to DEBUG for verbose logs
if app.config.get('ENV') == 'production':
    # The following is required to run on Azure App Service or any other host with reverse proxy:
    from werkzeug.middleware.proxy_fix import ProxyFix
    app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)
    # Use client credential from outside the config file, if available.
    if secure_client_credential: aad_configuration.client.client_credential = secure_client_credential

AADConfig.sanity_check_configs(aad_configuration)
adapter = FlaskContextAdapter(app)
ms_identity_web = IdentityWebPython(aad_configuration, adapter)


#Dash Apps
import dash_bootstrap_components as dbc
external_stylesheets = [dbc.themes.BOOTSTRAP]
pubmedDashApp = dash.Dash(__name__, server=app, url_base_pathname='/pub_dashboard/', external_stylesheets=external_stylesheets)
pubmedDashApp.layout= pubs.build_pubs_dash

youtubeDashApp = dash.Dash(__name__, server=app, url_base_pathname='/educ_dashboard/', external_stylesheets=external_stylesheets)
youtubeDashApp.layout= education.build_education_dash



@app.route('/')
@app.route('/sign_in_status')
def index():
    return render_template('home.html')
    # return render_template('auth/status.html')

@app.route('/update_all', methods=['GET'])
def update_all():
    youtube.main()
    pubmed.main()
    return 

@app.route('/publication_dashboard/', methods = ['POST', 'GET'])
def dashboard():
    # dashHtml = BeautifulSoup(pubmedDashApp.index(), 'html.parser')
    return render_template("publication_dashboard.html")
    # return jsonify({'htmlresponse': render_template('publication_dashboard.html', dashHtml = pubmedDashApp)})


@app.route('/pub_dashboard', methods = ['POST', 'GET'])
def dash_app_pub():
    return pubmedDashApp.index()


@pubmedDashApp.callback(
    Output(component_id='bar-container', component_property='children'),
    [Input(component_id='datatable-interactivity', component_property="derived_virtual_data"),
     Input(component_id='datatable-interactivity', component_property='derived_virtual_selected_rows'),
     Input(component_id='datatable-interactivity', component_property='derived_virtual_selected_row_ids'),
     Input(component_id='datatable-interactivity', component_property='selected_rows'),
     Input(component_id='datatable-interactivity', component_property='derived_virtual_indices'),
     Input(component_id='datatable-interactivity', component_property='derived_virtual_row_ids'),
     Input(component_id='datatable-interactivity', component_property='active_cell'),
     Input(component_id='datatable-interactivity', component_property='selected_cells')], prevent_initial_call=True
)
def update_bar(all_rows_data, slctd_row_indices, slct_rows_names, slctd_rows,
               order_of_rows_indices, order_of_rows_names, actv_cell, slctd_cell):

    dff = pd.DataFrame(all_rows_data)
    df2=((dff.groupby('Publication Year')['PubMed ID']).count()).reset_index()
    df2.columns=['Year','Count']
    df3=((dff.groupby('Publication Year')['Citation Count']).sum()).reset_index()
    df3['cumulative']= round(df3['Citation Count'].cumsum(), 0)
    
    df3.columns=['Year','citations','Count']
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add traces
    fig.add_trace(
        go.Bar(
            x=df2['Year'],
            y=df2['Count'],
            marker=dict(color = '#20425A'),
            hovertemplate =
                '<i>Publications in %{x}</i>: %{y:.0f}<extra></extra>',
            showlegend = False
            
            ), 
        secondary_y=False,
    )

    fig.add_trace(
        go.Line(
            x=df3['Year'],
            y=df3['Count'],
            marker=dict(color = '#f6ac15'),
            hovertemplate =
                '<i>Citations in %{x}</i>: %{y} <extra></extra>',
            ),
        secondary_y='Secondary'
    )

    # Add figure title
    fig.update_layout(title_text="<b> OHDSI Publications & Cumulative Citations</b>", title_x=0.5, showlegend=False)
    # Set x-axis title
    fig.update_xaxes(title_text="Year")
    # Set y-axes titles
    fig.update_yaxes(
        title_text="Number of Publications", 
        secondary_y=False)
    fig.update_yaxes(
        title_text="Number of Citations", 
        secondary_y=True)

    return [
        dcc.Graph(id = 'bar-chart', 
                    figure = fig.update_layout(yaxis={'tickformat': '{:,}'}),
                    style={'width': '100%', 'padding-left': '50px'},
                    )
            ]


@pubmedDashApp.callback(
    Output(component_id='line-container', component_property='children'),
    [Input(component_id='datatable-interactivity', component_property="derived_virtual_data"),
     Input(component_id='datatable-interactivity', component_property='derived_virtual_selected_rows'),
     Input(component_id='datatable-interactivity', component_property='derived_virtual_selected_row_ids'),
     Input(component_id='datatable-interactivity', component_property='selected_rows'),
     Input(component_id='datatable-interactivity', component_property='derived_virtual_indices'),
     Input(component_id='datatable-interactivity', component_property='derived_virtual_row_ids'),
     Input(component_id='datatable-interactivity', component_property='active_cell'),
     Input(component_id='datatable-interactivity', component_property='selected_cells')], prevent_initial_call=True
)
def update_author_bar(all_rows_data, slctd_row_indices, slct_rows_names, slctd_rows,
               order_of_rows_indices, order_of_rows_names, actv_cell, slctd_cell):

    currentAuthorSummaryTable = retrieveAuthorSummaryTable(key_dict, 'pubmed_author')
    currentAuthorSummaryTable = currentAuthorSummaryTable[['pubYear', 'numberNewFirstAuthors', 'cumulativeFirstAuthors', 'numberNewAuthors', 'cumulativeAuthors']]
    currentAuthorSummaryTable.columns = ['Year', 'New First Authors', 'Total First Authors', 'All New Authors', 'Total Authors']

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add traces
    fig.add_trace(
        go.Bar(
            x=currentAuthorSummaryTable['Year'],
            y=currentAuthorSummaryTable['All New Authors'],
            marker=dict(color = '#20425A'),
            hovertemplate =
                '<i>New Authors in %{x}</i>: %{y:.0f}<extra></extra>',
            showlegend = False
            
            ), 
        secondary_y=False,
    )

    fig.add_trace(
        go.Line(
            x=currentAuthorSummaryTable['Year'],
            y=currentAuthorSummaryTable['Total Authors'],
            marker=dict(color = '#f6ac15'),
            hovertemplate =
                '<i>Cumulative Authors by %{x}</i>: %{y} <extra></extra>',
            ),
        secondary_y='Secondary'
    )

    # Add figure title
    fig.update_layout(title_text="<b> New and Cumulative OHDSI Researchers</b>", title_x=0.5, showlegend=False)
    # Set x-axis title
    fig.update_xaxes(title_text="Year")
    # Set y-axes titles
    fig.update_yaxes(
        title_text="Number of New Authors", 
        secondary_y=False)
    fig.update_yaxes(
        title_text="Number of Cumulative Authors", 
        secondary_y=True)

    return [
        dcc.Graph(id = 'bar-chart', 
                    figure = fig.update_layout(yaxis={'tickformat': '{:,}'}),
                    style={'width': '100%', 'padding-left': '50px'},
                    )
            ]


@app.route('/education_dashboard/', methods = ['POST', 'GET'])
def dashboard_education():
    return render_template("education_dashboard.html")


@app.route('/educ_dashboard', methods = ['POST', 'GET'])
def dash_app_education():
    return youtubeDashApp.index()


@youtubeDashApp.callback(
    Output(component_id='bar-container', component_property='children'),
    [Input(component_id='datatable-interactivity', component_property="derived_virtual_data"),
     Input(component_id='datatable-interactivity', component_property='derived_virtual_selected_rows'),
     Input(component_id='datatable-interactivity', component_property='derived_virtual_selected_row_ids'),
     Input(component_id='datatable-interactivity', component_property='selected_rows'),
     Input(component_id='datatable-interactivity', component_property='derived_virtual_indices'),
     Input(component_id='datatable-interactivity', component_property='derived_virtual_row_ids'),
     Input(component_id='datatable-interactivity', component_property='active_cell'),
     Input(component_id='datatable-interactivity', component_property='selected_cells')], prevent_initial_call=True
)
def youtubeupdate_bar(all_rows_data, slctd_row_indices, slct_rows_names, slctd_rows,
               order_of_rows_indices, order_of_rows_names, actv_cell, slctd_cell):
    df = pd.DataFrame(all_rows_data)
    df=df[df.channelTitle.str.startswith('OHDSI')].copy(deep=True)
    # df['yr']=df['Date Published'].dt.year
    # df['Duration'] = education.convert_time(df['Duration'])
    df['Duration'] = df.apply(lambda x: x['Duration'][2:], axis = 1)
    df['Duration'] = df.apply(lambda x: education.convert_time(x['Duration']), axis = 1)

    from plotly.subplots import make_subplots
    import plotly.graph_objects as go
    # fig = make_subplots(rows=1, cols=2,
    #                     subplot_titles=("Youtube Hours Created","Cumulative Hrs Watched"))
    df4=df.groupby('yr').Duration.sum().reset_index()
    df4.columns=['Year','SumSeconds']
    df4['Hrs Created']=df4['SumSeconds'].dt.days*24 + df4['SumSeconds'].dt.seconds/3600

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add traces
    fig.add_trace(
        go.Bar(
            x=df4['Year'],
            y=df4['Hrs Created'],
            marker=dict(color = '#20425A'),
            hovertemplate =
                '<i>%{x}</i>: %{y:.0f} hours of content created <extra></extra>',
            showlegend = False
            
            ), 
        secondary_y=False,
    )
    # df['hrsWatched']=(df.Duration.dt.days*24+df.Duration.dt.seconds/3600)*df['Total Views']
    df4=df.groupby('yr').hrsWatched.sum().reset_index()
    df4.columns=['Year','HrsWatched']
    df4['Cumulative Hrs Watched']= np.round(df4['HrsWatched'].cumsum(), 0)
    # df4['Cumulative Hrs Watched'] = df4['Cumulative Hrs Watched'].apply(lambda x :int(x))
    # df4['Cumulative Hrs Watched'] = df4['Cumulative Hrs Watched'].apply(lambda x : "{:,}".format(x))
    fig.add_trace(
        go.Line(
            x=df4['Year'],
            y=df4['Cumulative Hrs Watched'],
            marker=dict(color = '#f6ac15'),
            hovertemplate =
                '<i>%{x}</i>: %{y} hours of video watched <extra></extra>'

            ),
            
        secondary_y='Secondary'
    )

    # Add figure title
    fig.update_layout(title_text="<b> YouTube Analysis </b>", title_x=0.5, showlegend=False)

    # Set x-axis title
    fig.update_xaxes(title_text="Year")

    # Set y-axes titles
    fig.update_yaxes(
        title_text="Content Hours Created", 
        secondary_y=False)
    fig.update_yaxes(
        title_text="Cumulative Hours Watched", 
        secondary_y=True)

    return [
        dcc.Graph(id = 'bar-chart', 
                    figure = fig.update_layout(yaxis={'tickformat': '{:,}'}),
                    style={'width': '100%', 'padding-left': '50px'},
                    )
            ]





#authentications


@app.route('/token_details')
@ms_identity_web.login_required # <-- developer only needs to hook up login-required endpoint like this
def token_details():
    current_app.logger.info("token_details: user is authenticated, will display token details")
    return render_template('auth/token.html')


@app.route('/not_found')
def not_found():
    return jsonify(message = 'That resource was not found'), 404
    

@app.route('/login', methods = ['POST'])
def login():
    email = request.form['email']
    password = request.form['password']

    if((email == "sliu197@jhmi.edu") and (password == "1111")):
        test = True
    else: 
        test = False
    if test:
        return jsonify(message = "True")
    else:
        return jsonify(message = 'Login unsuccessful. Bad email or password'), 401


#API Calls to CosmosDB


@app.route('/articleManager')
def articleManager():
    # count = 0
    # countIgnore = 0
    # listHolder = []
    # listHolderIgnore = []
    # for item in container.query_items( query='SELECT * FROM pubmed', enable_cross_partition_query=True):
    #     count += 1
    #     listHolder.append(item['data']['title'])
    
    # for item in container_ignore.query_items( query='SELECT * FROM pubmed_ignore', enable_cross_partition_query=True):
    #     countIgnore += 1
    #     listHolderIgnore.append(item['data']['title'])
    # return render_template("index.html",articles=listHolder )
    return render_template("articleManager.html")

@app.route("/fetchrecords",methods=["POST","GET"])
def fetchrecords():
    count = 0
    listHolder = []
    if request.method == 'POST':
        query = request.form['query']

        if(query == ''):
            for item in container.query_items( query='SELECT * FROM pubmed ORDER BY pubmed.data.pubYear DESC', enable_cross_partition_query=True):
                count += 1
                listHolder.append(item['data'])

        elif((query != '') ):
            search_text = "%" + request.form['query'] + "%"
            for item in container.query_items( 'SELECT * FROM pubmed WHERE ((LOWER(pubmed.data.title) LIKE LOWER(@searchStr)) or \
                                                                            (LOWER(pubmed.id) LIKE @searchStr) or \
                                                                                (LOWER(pubmed.data.firstAuthor) LIKE LOWER(@searchStr)) ) ORDER BY pubmed.data.pubYear DESC',
                                                [{"name": "@searchStr", "value": search_text}], enable_cross_partition_query=True
                                                ):
                count += 1
                listHolder.append(item['data'])
    # return jsonify("success")
    return jsonify({'htmlresponse': render_template('response.html', articleList=listHolder, numArticle = count)})

@app.route("/insert",methods=["POST","GET"])
def insert():
    dateMY = "" + date.datetime.now().strftime("%m-%d-%Y")[0:2] + date.datetime.now().strftime("%m-%d-%Y")[5:10]
    if request.method == 'POST':
        
        searchArticles = request.form['articleIdentifier']
        designatedContainer = request.form['containerChoice']
        numNewArticles = 0
        containerArticles = getExistingIDandSearchStr(key_dict, designatedContainer)

        secret_api_key = key_dict['SERPAPI_KEY'] #SERPAPI key
        articleTable = getPMArticles(searchArticles)
        articleTable = articleTable[articleTable['pubYear'] > 2010]
        try:
            specifiedArticle = articleTable['pubmedID'][0]
        except KeyError:
            return jsonify("This article may not be officially available in the system yet. Check back again...")
        else:

            specifiedArticle = articleTable['pubmedID'][0]
            articleTable = articleTable[articleTable.pubmedID.notnull()]
            articleTable, numNewArticles = identifyNewArticles(articleTable, key_dict)

            if(numNewArticles == 0):
                if(specifiedArticle in containerArticles[0]):
                    return jsonify("This article already exists in the '" + str(designatedContainer) + "' container. Please verify." )
                else:
                    return jsonify("This article already exists in the other container. Please verify." )
            else:
                
                articleTable[['foundInGooScholar', 'numCitations', 'levenProb', 'fullAuthorGooScholar', 'googleScholarLink']] = articleTable.apply(lambda x: getGoogleScholarCitation(x, secret_api_key), axis = 1, result_type='expand')
                articleTable = articleTable.reset_index()
                if ('index' in articleTable.columns):
                    del articleTable['index']

                #update the current records
                # makeCSVJSON(finalTable, key_dict)
                #update the current records
                makeCSVJSON(articleTable, key_dict, designatedContainer, False)


                return jsonify("" + str(numNewArticles) + " new article(s) added successfully")


@app.route('/remove_article', methods=['DELETE'])
def remove_article():
    
    if request.method == 'DELETE':
        searchArticles = request.form['articleIDToRemove']
        designatedContainer = request.form['containerWithArticle']
        containerArticles = getExistingIDandSearchStr(key_dict, designatedContainer)
        # print(searchArticles)
        # print(designatedContainer)
        if(designatedContainer == "pubmed_ignore"):
            if(searchArticles in containerArticles[0]):
                for item in container_ignore.query_items( 'SELECT * FROM pubmed_ignore', enable_cross_partition_query=True):
                    if(item['id'] == ("PMID: " + str(searchArticles))):
                        container_ignore.delete_item(item=item, partition_key=item['id'])
            else:
                return jsonify('Article does not exist in this container.')
        else:
            if(searchArticles in containerArticles[0]):
                for item in container.query_items( 'SELECT * FROM pubmed', enable_cross_partition_query=True):
                    if(item['id'] == ("PMID: " + str(searchArticles))):
                        container.delete_item(item=item, partition_key=item['id'])
            else: 
                return jsonify('Article does not exist in this container.')

        return jsonify('Article removed.')

    
@app.route('/moveToContainer', methods=['POST'])
def moveToContainer():
    if(request.method == 'POST'):
        articleToMove = request.form['articleMove']
        containerArticles = getExistingIDandSearchStr(key_dict, 'pubmed')
        ignoreArticles = getExistingIDandSearchStr(key_dict, 'pubmed_ignore')

        if(articleToMove in containerArticles[0]):
            moveItemToIgnoreContainer(key_dict, [articleToMove], 'pubmed', 'pubmed_ignore')
            return jsonify("Article moved to the ignore container.")
        elif(articleToMove in ignoreArticles[0]):
            moveItemToIgnoreContainer(key_dict, [articleToMove], 'pubmed_ignore', 'pubmed')
            return jsonify("Article moved to the pubmed article container.")
        else:
            return jsonify("Article is not in the database. Add it first.")


if __name__ == '__main__':
    app.run(debug=True)
    # dashApp.run_server(host='0.0.0.0', debug=True)
