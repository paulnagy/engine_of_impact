from api_miners import key_vault, youtube, pubmed
from api_miners.pubmed import *
from azure.cosmos import CosmosClient, PartitionKey
import dash
from dash import Dash, dcc, html, Input, Output, State
import datetime as date   
from googleapiclient.discovery import build
from flask import Flask
from flask_session import Session
from flask import Flask, current_app, flash, jsonify, make_response, redirect, request, render_template, send_file, Blueprint, url_for, redirect
from functools import wraps
import logging
from ms_identity_web import IdentityWebPython
from ms_identity_web.adapters import FlaskContextAdapter
from ms_identity_web.configuration import AADConfig
import plotly.express as px
import pandas as pd 
#dashapp layouts
from views import pubs, education

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

#youtube.main()
#pubmed.main()
# app=dash.Dash()
#app.layout= pubs.build_pubs_dash()
# app.layout= education.build_education_dash()


#Dash Apps
pubmedDashApp = dash.Dash(__name__, server=app, url_base_pathname='/pub_dashboard/')
pubmedDashApp.layout= pubs.build_pubs_dash

youtubeDashApp = dash.Dash(__name__, server=app, url_base_pathname='/education_dashboard/')
youtubeDashApp.layout= education.build_education_dash



@app.route('/')
@app.route('/sign_in_status')
def index():
    return render_template('mainContent.html')
    # return render_template('auth/status.html')


@app.route('/publication_dashboard/', methods = ['POST', 'GET'])
def dashboard():
    # dashHtml = BeautifulSoup(pubmedDashApp.index(), 'html.parser')
    return render_template("publication_dashboard.html")
    # return jsonify({'htmlresponse': render_template('publication_dashboard.html', dashHtml = pubmedDashApp)})



@app.route('/pub_dashboard', methods = ['POST', 'GET'])
def dash_app_pub():
    return pubmedDashApp.index()


@pubmedDashApp.callback(
    Output('container-button-basic', 'children'),
    Input('input-on-submit', 'value')
)
def update_output(value):
    if(value != ""):
        searchArticles = value
        designatedContainer = "pubmed"
        numNewArticles = 0
        containerArticles = getExistingIDandSearchStr(key_dict, designatedContainer)
        secret_api_key = key_dict['SERPAPI_KEY'] #SERPAPI key
        articleTable = getPMArticles(searchArticles)
        articleTable = articleTable[articleTable['pubYear'] > 2010]
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
            
        value = ""
        return '{} new article(s) added successfully'.format(numNewArticles)
        # return pubmedDashApp.index()
        # return dashboard()


@app.route('/education_dashboard', methods = ['POST', 'GET'])
def education():
    return youtubeDashApp.index()


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

@app.route('/articleManager', methods = ['POST', 'GET'])
def articleManager():
    count = 0
    countIgnore = 0
    listHolder = []
    listHolderIgnore = []
    for item in container.query_items( query='SELECT * FROM pubmed', enable_cross_partition_query=True):
        count += 1
        listHolder.append(item['data']['title'])
    
    for item in container_ignore.query_items( query='SELECT * FROM pubmed_ignore', enable_cross_partition_query=True):
        countIgnore += 1
        listHolderIgnore.append(item['data']['title'])
    return render_template("index.html",articles=listHolder )

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
