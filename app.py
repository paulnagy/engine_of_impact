import dash
from dash import dcc, html, dash_table
from api_miners import key_vault, youtube, pubmed
from views import pubs, education
import plotly.express as px
import pandas as pd 
from api_miners.pubmed import *
#youtube.main()
#pubmed.main()
# app=dash.Dash()
#app.layout= pubs.build_pubs_dash()
# app.layout= education.build_education_dash()


from flask import Flask
from functools import wraps
from azure.cosmos import CosmosClient,PartitionKey
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from oauth2client.tools import argparser

from Bio import Entrez, Medline #http://biopython.org/DIST/docs/tutorial/Tutorial.html#sec%3Aentrez-specialized-parsers
import xmltodict #https://marcobonzanini.com/2015/01/12/searching-pubmed-with-python/
import time
import datetime as date   
import numpy as np
import json
import re
from serpapi import GoogleSearch
import csv
import Levenshtein as lev
from fuzzywuzzy import fuzz, process
from os.path import exists
from pprint import pprint
from collections import defaultdict, Counter
from dateutil.parser import *
from email.headerregistry import ContentTypeHeader
import mimetypes
from httplib2 import Response
#
import logging
from pickle import GET, TRUE
from re import L
from flask import Flask, current_app, flash, jsonify, make_response, redirect, request, render_template, send_file, Blueprint, url_for, redirect
import os
from io import StringIO

from ms_identity_web import IdentityWebPython
from ms_identity_web.adapters import FlaskContextAdapter
from ms_identity_web.configuration import AADConfig
# credential = DefaultAzureCredential()()
# subscription_client = SubscriptionClient(credential)

app = Flask(__name__)
key_dict = key_vault.get_key_dict()
endpoint = key_dict['AZURE_ENDPOINT']
azure_key = key_dict['AZURE_KEY']
secret_api_key = key_dict['SERPAPI_KEY']

client = CosmosClient(endpoint, azure_key)
database_name = 'ohdsi-impact-engine'
container = pubmed.init_cosmos(key_dict, 'pubmed')
container_ignore = pubmed.init_cosmos(key_dict, 'pubmed_ignore')


dashApp=dash.Dash(__name__,
    server=app,
    url_base_pathname='/dashboard/')
dashApp.layout= pubs.build_pubs_dash()

aad_configuration = AADConfig.parse_json('aadconfig.json')
adapter = FlaskContextAdapter(app)
ms_identity_web = IdentityWebPython(aad_configuration, adapter)

# @app.route('/')
# def home():
#     return render_template('login.html')
#     # return 'Welcome to Engine-of-Impact: OHDSI Article Manager '

@app.route('/')
@app.route('/sign_in_status')
def index():
    return render_template('auth/status.html')


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
    # test = User.query.filter_by(email=email, password = password).first()

    if test:

        return jsonify(message = "True")
        
        
    else:
        return jsonify(message = 'Login unsuccessful. Bad email or password'), 401

@app.route('/articleManager', methods = ['POST', 'GET'])# @jwt_required()
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
            
            articleTable[['foundInGooScholar', 'numCitations', 'levenProb', 'fullAuthorGooScholar']] = articleTable.apply(lambda x: getGoogleScholarCitation(x), axis = 1, result_type='expand')
            articleTable = trackCitationChanges(articleTable, key_dict)
            if ('Unnamed: 0' in articleTable.columns):
                del articleTable['Unnamed: 0']
                del articleTable['Unnamed: 0.1']
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
                print("in pubmed container")
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

@app.route('/dashboard', methods = ['POST', 'GET'])
def dashboard():
    return dashApp.index()



if __name__ == '__main__':
    app.run(debug=True)
    # dashApp.run_server(host='0.0.0.0', debug=True)
