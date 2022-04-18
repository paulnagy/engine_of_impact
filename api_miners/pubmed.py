from azure.cosmos import CosmosClient,PartitionKey
from apiclient.discovery import build
from apiclient.errors import HttpError
from oauth2client.tools import argparser
from api_miners import key_vault as kv

from Bio import Entrez, Medline #http://biopython.org/DIST/docs/tutorial/Tutorial.html#sec%3Aentrez-specialized-parsers
import xmltodict #https://marcobonzanini.com/2015/01/12/searching-pubmed-with-python/
import time
import datetime as date   
import pandas as pd
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
import ast

def init_cosmos(key_dict: dict, container_name:str):
    """Initialize the Cosmos client
    Parameters
    ---
    * container_name : str - Name of azure container in cosmos db

    Returns container for cosmosclient
    """
    endpoint = key_dict['AZURE_ENDPOINT']
    azure_key = key_dict['AZURE_KEY']
    client = CosmosClient(endpoint, azure_key)
    database_name = 'ohdsi-impact-engine'
    database = client.create_database_if_not_exists(id=database_name)
    container = database.create_container_if_not_exists(
        id=container_name, 
        partition_key=PartitionKey(path="/id"),
        offer_throughput=400
    )
    return container

def pubmedAPI(searchQuery):
    """
    Called in getPMArticles()
    For each of the search terms (searchQuery), search on pubmed and pmc databases
    Convert the results into a dataframe
    """
    Entrez.email = 'sliu197@jhmi.edu' #personal email address for Pubmed to reach out if necessary
    paramEutils = { 'usehistory':'Y' } #using cache
    queryList = searchQuery
    dbList = ['pubmed', 'pmc'] #Search through all databases of interest 'nlmcatalog', 'ncbisearch' 
    articleList = [] #empty placeholder
    retMax = 1000 #number of results to return
    
    if(type(queryList) == list):
        for searchStringItem in queryList:
            # generate query to Entrez eSearch
            eSearch = Entrez.esearch(db="pubmed", term=searchStringItem, **paramEutils, retmax = retMax)

            # get eSearch result as dict object
            res = Entrez.read(eSearch)
            idList = res["IdList"]
            handle = Entrez.efetch(db="pubmed",
                                   id=idList, rettype="medline", retmode="json", retmax = retMax)
            articleListTemp = Medline.parse(handle)
            articleListTemp = list(articleListTemp)
            articleList.extend(articleListTemp)
        print("Found in PubMed:", len(articleList))
    else:
        for dB in dbList: 
                # generate query to Entrez eSearch
                eSearch = Entrez.esearch(db=dB, term=searchQuery, **paramEutils, retmax = retMax)

                # get eSearch result as dict object
                res = Entrez.read(eSearch)
                idList = res["IdList"]
                handle = Entrez.efetch(db="pubmed",
                                       id=idList, rettype="medline", retmode="json", retmax = retMax)
                articleListTemp = Medline.parse(handle)
                articleListTemp = list(articleListTemp)
                articleList.extend(articleListTemp)
                print(dB, ":", len(articleListTemp))

    print("Total number of articles :", len(articleList))

    #reshape the table
    outputTable = pd.DataFrame.from_dict(articleList[0], orient = "index")
    for i in range(len(articleList)-1):
        row = pd.DataFrame.from_dict(articleList[i+1], orient = "index")
        outputTable = pd.concat([outputTable, row], axis = 1)
    outputTable = outputTable.T
    return outputTable

def selectAndDropCol(table):
    """
    Called in getPMArticles()
    Rename, select, and drop columns
    """
    outputTable = table.rename(columns={"AB": "abstract", "CI": "copyrightInformation", "AD": "affiliation",
                               "IRAD": "investigatorAffiliation", "AID": "articleID", "AU": "author",
                               "AUID": "authorID", "FAU": "fullAuthor", "BTI": "bookTitle",
                               "CTI": "collectionTitle", "COIS": "confOfInterest", "CN": "corporateAuthor",
                               "CRDT": "creationDate", "DCOM": "dtAddedToDB", "DA": "dtProcesCreated",
                               "LR": "lastRevised", "DEP": "dtOfElecPub", "DP": "dtOfPub",
                               "EN": "edition", "ED": "editorName", "FED": "fullEditorName",
                               "EDAT": "dtCitationAdded", "GS": "geneSymbol", "GN": "generalNote",
                               "GR": "grantNum", "IR": "investName", "FIR": "fullInvestName",
                               "ISBN": "isbn", "IS": "issn", "IP": "issue",
                               "TA": "journalTitleAbbrev", "JT": "journalTitle", "LA": "language",
                               "LID": "locID", "MID": "manuID", "MHDA": "dtMeshAddedtoCitation", 
                               "MH": "meshT", "JID": "nlmID", "RF": "numOfRefer", 
                               "OAB": "othAbstract", "OCI": "othCopyRInfo", "OID": "otherID", 
                               "OT": "othTerm", "OTO": "othTermOwner", "OWN": "owner", 
                               "PG": "pagination", "PS": "personalNameAsSubject", "FPS": "fullpNAS", 
                               "PL": "countryOfPub", "PHST": "pubHistStatus", "PST": "pubStatus",
                               "PT": "pubType", "PUBM": "pubModel", "PMC": "pmcID",
                               "PMCR": "pmcRelease", "PMID": "pubmedID",
                               "RN": "registryNum", "NM": "suppleConceptRecord", "SI": "secondSoID",
                               "SO": "source", "SFM": "spaceFlightMission", "STAT": "status",
                               "SB": "subset", "TI": "title", "TT": "translitTitle",
                               "VI": "volume", "VTI": "volumeTitle"
                              })

    #affiliation, author, authorID, fullAuthor, creationDate, grantNum, investName, fullInvestName, language, locID, nlmID
    #numOfRefer(ences), countryOfPub(lication), pmcID, pubmedID, source, title
    listOfCol = ["pmcID", "pubmedID", "nlmID", "journalTitle", "title",  "creationDate", "affiliation",
                   "locID", "countryOfPub", "language", "grantNum", "fullAuthor", "meshT", "source"]
    #for any missing column, create it
    for i in range(len(listOfCol)):
        if ((listOfCol[i] in outputTable.columns) == False):
            outputTable[listOfCol[i]] = None
    outputTable = outputTable.drop_duplicates('pubmedID', keep = 'first')[["pmcID", "pubmedID", "nlmID", 
                                                                         "journalTitle",
                                                                         "title", 
                                                                          "creationDate", "affiliation", 
                                                                         "locID", "countryOfPub", "language",
                                                                         "grantNum", "fullAuthor", "meshT", 
                                                                         "source"]]
    outputTable = outputTable.reset_index()
    outputTable = outputTable.drop(columns = ['index'])
    return outputTable

def formatName(row):
    """
    Called in getPMArticles()
    Format all the names into "first, last" so that it is consistent with Google Scholars
    """
    fullAuthorStr = str(row['fullAuthor'])
    i = 0
    startQ = 0
    sepQ = 0
    endQ = 0
    numComma = 0
    replacement = "["
    while( i < len(fullAuthorStr)):
        if(fullAuthorStr[i] == "'"):
            numComma += 1
            if(numComma % 2 == 0):
                endQ = i
                replacement = replacement + "'" + str(fullAuthorStr[(sepQ + 2):(endQ)]) + ", " + str(fullAuthorStr[(startQ + 1):(sepQ)]) + "'"
                if(endQ + 2 != len(fullAuthorStr)):
                    replacement = replacement + ", "
                    i += 3

            else:
                startQ = i
                i += 1
        elif(fullAuthorStr[i] == ","):
            sepQ = i
            i += 1

        else:
            i += 1
    replacement = replacement + "]"
    return replacement

def splitFullAuthorColumn(outputTable):
    """
    Called in getPMArticles()
    Split the full author column into first author, ...etc. keep the first author column only
    """
    # testFind = re.sub('[^A-Za-z0-9]+', '', testFind).lower()
    outputTable['fullAuthor'] = outputTable['fullAuthor'].astype("str")
    outputTable['fullAuthorEdited'] = outputTable['fullAuthor'].map(lambda fullAuthor: re.sub(',', '', fullAuthor[1:len(fullAuthor)-1]))

    # re.sub('([^"]|\\")*', '', finalTable['fullAuthor']).lower()
    tempSplit = outputTable['fullAuthorEdited'].str.split("' '", n = -1, expand = True)
    tempSplit = tempSplit.add_prefix('author')
    outputTable = pd.concat([outputTable, tempSplit], axis = 1)
    # finalTable['fullAuthor'][13]
    return outputTable

def getYear(row):
    """
    Called in getPMArticles()
    Get the year published for every article. 
    Used for filtering articles after 2010
    """
    if(row['creationDate'] == 'nan'):
        return(2000)
    else:
        return(pd.to_numeric(row['creationDate'][0:4]))
    
def getPMArticles(query):
    """
    Called in main()
    Fetch relevant articles from PubMed
    Perform final cleaning on this dataframe before it is passed for google scholar
        citation search and match (getGoogleScholarCitation() applied to each row)
    """
    outputTable = pubmedAPI(query)
    outputTable = selectAndDropCol(outputTable)
    outputTable['fullAuthor'] = outputTable.apply(formatName, axis = 1)
    outputTable = splitFullAuthorColumn(outputTable)
    
    outputTable['creationDate'] = outputTable['creationDate'].astype(str)
    outputTable['creationDate'] = outputTable.apply(lambda x: x['creationDate'][2:-2], axis = 1)
    outputTable['pubYear'] = outputTable.apply(lambda x: getYear(x), axis = 1)
    outputTable['titleAuthorStr'] = "" + outputTable['title'] + " " + outputTable['author0']
    outputTable['datePulled'] = date.datetime.now().strftime("%m-%d-%Y")

    outputTable = outputTable.rename(columns = {
       "author0": "firstAuthor"
    })
    outputTable = outputTable[outputTable.columns.drop(list(outputTable.filter(regex='author[0-9]+')))]
    if ('Unnamed: 0' in outputTable.columns):
        del outputTable['Unnamed: 0']
    
    return outputTable

def serpAPI(query, api_key):
    """
    Called in getGoogleScholarCitation()
    Input API key and search term
    """
    fromYr = 2010
    params = {
      "engine": "google_scholar",
      "q": query,
      "hl": "en",
      "start": 0,
      "num": "20",
      "api_key": api_key
    }
    search = GoogleSearch(params)
    results = search.get_dict()
    
    return results

def saveRawSerpApiAsDict(serpRawResult):
    """
    Called in getGoogleScholarCitation()
    concatenate all the results into one JSON object
    """
    extractedResult = {}
    if('organic_results' in serpRawResult.keys()):
        lengthResult = len(serpRawResult['organic_results'])
        if(lengthResult == 1):
            extractedResult = {'gScholarQResults': [serpRawResult['organic_results'][0]]}
        if(lengthResult > 1):
            extractedResult = {'gScholarQResults': [serpRawResult['organic_results'][0]]}
            for k in range(lengthResult-1):
                extractedResult['gScholarQResults'].append(serpRawResult['organic_results'][k+1])
                
    return extractedResult

def serpApiExtract(extractedResult):
    """
    Called in getGoogleScholarCitation()
    From the JSON object produced by saveRawSerpApiAsDict(), 
        extract title, author, and citation information based on rules.
    """
    searchDict = {"citationInfo": {}, 'firstAuthorInfo': {}, 'fullAuthorInfo': {}, 'titleAuthorStr': {}, 'citationChangesSinceLast': {}}
    dashIndex = 0
    #if there is more than one article returned
    if(len(extractedResult['gScholarQResults']) < 2):
        #get the title
        title = extractedResult['gScholarQResults'][0]['title']
        #if there is no citation information, set to 0
        if(('cited_by' in extractedResult['gScholarQResults'][0]['inline_links'].keys()) == False):
            searchDict['citationInfo'][title] = 0
        else:
            numCitedBy = extractedResult['gScholarQResults'][0]['inline_links']['cited_by']['total']
            searchDict['citationInfo'][title] = numCitedBy

        #find author(s) if it is populated
        if(('authors' in extractedResult['gScholarQResults'][0]['publication_info']) == False):
            searchDict['firstAuthorInfo'][title] = "No information available"
            if((('summary' in extractedResult['gScholarQResults'][0]['publication_info']) == True)):
                if("-" in extractedResult['gScholarQResults'][0]['publication_info']['summary']):
                    dashIndex = extractedResult['gScholarQResults'][0]['publication_info']['summary'].index("-")
                    searchDict['fullAuthorInfo'][title] = extractedResult['gScholarQResults'][0]['publication_info']['summary'][0:dashIndex]
                else:
                    searchDict['fullAuthorInfo'][title] = "No information available"
            else:
                searchDict['fullAuthorInfo'][title] = "No information available"
        else:
            findAuthors = extractedResult['gScholarQResults'][0]['publication_info']['authors']
            resultStr = "["
            for i in range(len(findAuthors)):
                if (i == len(findAuthors) - 1):
                    resultStr = resultStr + "'" + findAuthors[i]['name'] + "'"
                else:
                    resultStr = resultStr + "'" + findAuthors[i]['name'] + "', "

            resultStr = resultStr + "]"
            searchDict['firstAuthorInfo'][title] = findAuthors[0]['name']
            searchDict['fullAuthorInfo'][title] = resultStr
    
    else:
        for i in range(len(extractedResult['gScholarQResults'])):
            title = extractedResult['gScholarQResults'][i]['title']

            #check if the keys under inline_links contain cited by, if not set to 0.
            if(('cited_by' in extractedResult['gScholarQResults'][i]['inline_links'].keys()) == False):
                #check if it already exists
                if(title in searchDict['citationInfo'].keys()):
                    #if it does, do nothing, otherwise add it
                    if(i+2 < len(extractedResult['gScholarQResults'])):
                        i += 1
                        title = extractedResult['gScholarQResults'][i]['title']
                #otherwise, set to 0
                else:
                    searchDict['citationInfo'][title] = 0
            else:
                if(title in searchDict['citationInfo'].keys()):
                    if(searchDict['citationInfo'][title] > 0):
                        if(i+2 < len(extractedResult['gScholarQResults'])):
                            i += 1
                            title = extractedResult['gScholarQResults'][i]['title']
                    else:
                        numCitedBy = extractedResult['gScholarQResults'][i]['inline_links']['cited_by']['total']
                        searchDict['citationInfo'][title] = numCitedBy
                else:
                    numCitedBy = extractedResult['gScholarQResults'][i]['inline_links']['cited_by']['total']
                    searchDict['citationInfo'][title] = numCitedBy

            #find author(s) if it is populated
            if(('authors' in extractedResult['gScholarQResults'][i]['publication_info']) == False):
                searchDict['firstAuthorInfo'][title] = "No information available"
                if((('summary' in extractedResult['gScholarQResults'][i]['publication_info']) == True)):
                    if("-" in extractedResult['gScholarQResults'][i]['publication_info']['summary']):
                        dashIndex = extractedResult['gScholarQResults'][i]['publication_info']['summary'].index("-")
                        searchDict['fullAuthorInfo'][title] = extractedResult['gScholarQResults'][i]['publication_info']['summary'][0:dashIndex]
                    else:
                        searchDict['fullAuthorInfo'][title] = "No information available"
                else:
                    searchDict['fullAuthorInfo'][title] = "No information available"
            else:
                findAuthors = extractedResult['gScholarQResults'][i]['publication_info']['authors']
                resultStr = "["
                for i in range(len(findAuthors)):
                    if (i == len(findAuthors) - 1):
                        resultStr = resultStr + "'" + findAuthors[i]['name'] + "'"
                    else:
                        resultStr = resultStr + "'" + findAuthors[i]['name'] + "', "

                resultStr = resultStr + "]"
                searchDict['firstAuthorInfo'][title] = findAuthors[0]['name']
                searchDict['fullAuthorInfo'][title] = resultStr

    #populate the last dictionary with full title + first author for better levenshtein matching
    for key in searchDict['firstAuthorInfo']:
        if(searchDict['firstAuthorInfo'][key] != "No information available"):
            newStr = "" + key + " " + searchDict['firstAuthorInfo'][key]
        else: 
            newStr = "" + key
        searchDict['titleAuthorStr'][newStr] = key

    return searchDict

def getGoogleScholarCitation(row,serp_api_key):
    """
    Called in main() and applied to each row of the table output from getPMArticles()
    Output into 4 new columns: 
        title found on Google Scholar (could have different capitalization, abbrevation, spacing...etc), 
        number of citation, 
        levenshtein probability, and 
        a list of full authors from Google Scholar
    
    """
    searchTitle = str(row['titleAuthorStr']) #get search string
    results = serpAPI(searchTitle, serp_api_key) #search on google scholar
    appendedResults = saveRawSerpApiAsDict(results) #save results as json dictionary
    
    #create 4 new columns
    if(len(appendedResults) == 0):
        dictArticlesToMatch = {}
    else:
        dictArticlesToMatch = serpApiExtract(appendedResults)
    strOptions = dictArticlesToMatch['titleAuthorStr'].keys()
    if(len(strOptions) == 0):
        result = ["NA", "NA", "NA", "NA"]
        return result
    
    elif(len(strOptions) == 1):
        title = list(dictArticlesToMatch['titleAuthorStr'].values())[0]
        levenP = fuzz.token_set_ratio(searchTitle, list(dictArticlesToMatch['titleAuthorStr'].keys())[0])
        result = [title, dictArticlesToMatch['citationInfo'][title], levenP, dictArticlesToMatch['fullAuthorInfo'][title]]
        return result
    
    elif(len(strOptions) > 1):
        result = process.extractOne(str(searchTitle), strOptions) #extract the highest probability of match
        title = dictArticlesToMatch['titleAuthorStr'][result[0]]
        result = [title, dictArticlesToMatch['citationInfo'][title], result[1], dictArticlesToMatch['fullAuthorInfo'][title]]
        return result

def getLastUpdatedCitations(key_dict: dict, containerName):
    """
    Called in trackCitationChanges()
    Retrieve the number of citation for each article from the most recent update.
    """
    container = init_cosmos(key_dict, containerName)
    lastUpdateResults = {'citationInfo': {}}
    numCitation = 0
    for item in container.query_items(query=str('SELECT * FROM ' + containerName), enable_cross_partition_query=True):
        title = item['data']['title']
        numCitation = item['data']['trackingChanges'][len(item['data']['trackingChanges'])-1]['numCitations']
        lastUpdateResults['citationInfo'][title] = numCitation
    return lastUpdateResults

def trackCitationChanges(table, key_dict: dict):
    """
    Called in main()
    Calculate the change in the number of citations
    """
    #format the input table into a dictionary of {title: citation count}
    table = table.reset_index()
    if ('index' in table.columns):
        del table['index']
    retrievalResults = {"citationInfo": {}}
    tableLength = len(table['title'])
    for row in range(tableLength):
        retrievalResults['citationInfo'][table['title'][row]] = int(table['numCitations'][row])
    table['additionalCitationCount'] = 0
    #retrieve the counts from the last update
    lastUpdateResults = getLastUpdatedCitations(key_dict, 'pubmed')
    for key in (getLastUpdatedCitations(key_dict, 'pubmed_ignore')['citationInfo']):
        lastUpdateResults['citationInfo'][key] = getLastUpdatedCitations(key_dict, 'pubmed_ignore')['citationInfo'][key]
    for key in retrievalResults['citationInfo']:
        if(key in lastUpdateResults.keys()):
            table.loc[table['title'] == key, 'additionalCitationCount'] = retrievalResults['citationInfo'][key] - lastUpdateResults['citationInfo'][key]
        else:
            table.loc[table['title'] == key, 'additionalCitationCount'] = 0
    return table

def fetchCurrentDataAndUpdate(key_dict: dict, containerName):
    """
    Called in makeCSVJSON()
    Retrieve all existing records and add in updates
    """
    container = init_cosmos(key_dict, containerName)
    result = defaultdict(list)
    for item in container.query_items(query = str('SELECT * FROM ' + containerName), enable_cross_partition_query=True):
        result[item['id']] = item['data']
    return result


def makeCSVJSON(table, key_dict: dict):
    """
    Called in main()
    Add new records to the existing records and update container
    """
    container = init_cosmos(key_dict, 'pubmed')
    container_ignore = init_cosmos(key_dict, 'pubmed_ignore')
    data = fetchCurrentDataAndUpdate(key_dict, 'pubmed')
    data_ignore = fetchCurrentDataAndUpdate(key_dict, 'pubmed_ignore')
    for key in data_ignore:
        data[key] = data_ignore[key]
    d_timeseries = defaultdict(list)
    
    #format table into dictionary
    for row in range(len(table['pubmedID'])):
        d_trackingChanges = {}
        d_articleInfo = {}
        for k in table.columns:  
            if (k == 'pubmedID'):
                d_articleInfo[k] = str(int(float(table[k][row])))
            elif (k in ['pmcID', 'nlmID', 'journalTitle', 'title', 'creationDate', 'affiliation', 'locID', 'countryOfPub', 'language',
                        'grantNum', 'fullAuthor', 'source', 'fullAuthorEdited', 'firstAuthor', 'meshT',
                        'titleAuthorStr', 'foundInGooScholar', 
                        'fullAuthorGooScholar']):
                d_articleInfo[k] = str(table[k][row])
            elif( k in ['pubYear', 'levenProb']):
                d_articleInfo[k] = int(float(table[k][row]))
            elif( k in ['additionalCitationCount', 'numCitations', 'datePulled']):
                d_trackingChanges['t'] = int(parse(table["datePulled"][row]).timestamp())
                if( k == 'datePulled'):
                    d_trackingChanges[k] = str(table[k][row])
                else:
                    d_trackingChanges[k] = int(float(table[k][row]))


        id = "PMID: " + str(int(float(table['pubmedID'][row])))
        if(id in data.keys()):    
            if((id in d_timeseries.keys()) == False):
                for i in range(len(data[id]['trackingChanges'])):
                    d_timeseries[id].append(data[id]['trackingChanges'][i])
        d_timeseries[id].append(d_trackingChanges) 
        data[id] = d_articleInfo
        data[id]['trackingChanges'] = d_timeseries[id]
    
    data = dict(data)
    #for articles in the ignore list, add to the ignore container; otherwise, add to the pubmed container
    ignore_list = getExistingIDandSearchStr(key_dict, 'pubmed_ignore')[0]
    for k, v in data.items(): 
        if(k[6:len(k)] in ignore_list):
            container_ignore.upsert_item({
                    'id': k,
                    'data': v
                }
            )
        
        else:
            container.upsert_item({
                    'id': k,
                    'data': v
                }
            )

def getTimeOfLastUpdate(key_dict: dict):
    """
    Called in main()
    Not every article has the same last date of update. Find the most recent among all articles. 
    """
    container = init_cosmos(key_dict, 'pubmed')
    dateOfLastUpdate = "01-01-2022"
    for item in container.query_items(query='SELECT * FROM beta', enable_cross_partition_query=True):
        if(dateOfLastUpdate < item['data']['trackingChanges'][len(item['data']['trackingChanges'])-1]['datePulled']):
            dateOfLastUpdate = item['data']['trackingChanges'][len(item['data']['trackingChanges'])-1]['datePulled']
    return dateOfLastUpdate

def getExistingIDandSearchStr(key_dict: dict, containerName):
    """
    Called in main()
    Get a list of PMIDs and a list of title-author search strings
    Two outputs
    """
    container = init_cosmos(key_dict, containerName)
    result = []
    exisitingIDs = []
    exisitingTitleAuthorStr = []
    for item in container.query_items(query=('SELECT * FROM ' + containerName), enable_cross_partition_query=True):
        exisitingIDs.append(item['data']['pubmedID'])
        exisitingTitleAuthorStr.append(item['data']['titleAuthorStr'])
    result = [exisitingIDs, exisitingTitleAuthorStr]

    return result

def retrieveAsTable(key_dict: dict, beforeCalculateCitationChanges: bool, containerName):
    """
    Retrieves the data as a dataframe
    
    """
    container = init_cosmos(key_dict, containerName)
    pmcID, pubmedID, nlmID, journalTitle, title = [],[],[],[],[]
    creationDate, affiliation, locID, countryOfPub, language = [],[],[],[],[]
    grantNum, fullAuthor, meshT, source, fullAuthorEdited = [],[],[],[],[]
    firstAuthor, pubYear, titleAuthorStr, datePulled = [],[],[],[]
    if(beforeCalculateCitationChanges == False):
        foundInGooScholar, numCitations , levenProb, fullAuthorGooScholar = [],[],[],[]

    colNames = ['pmcID', 'pubmedID', 'nlmID', 'journalTitle', 'title',
           'creationDate', 'affiliation', 'locID', 'countryOfPub', 'language',
           'grantNum', 'fullAuthor', 'meshT', 'source', 'fullAuthorEdited',
           'firstAuthor', 'pubYear', 'titleAuthorStr', 'datePulled',
            'foundInGooScholar','numCitations', 'levenProb', 'fullAuthorGooScholar']
    if(beforeCalculateCitationChanges):
        colNames = colNames[0:19]

    for item in container.query_items(query = str('SELECT * FROM ' + containerName), enable_cross_partition_query=True):

        for i in range(len(item['data']['trackingChanges'])):
            pmcID.append(item['data']['pmcID'])
            pubmedID.append(item['data']['pubmedID'])
            nlmID.append(item['data']['nlmID'])
            journalTitle.append(item['data']['journalTitle'])
            title.append(item['data']['title'])

            creationDate.append(item['data']['creationDate'])
            affiliation.append(item['data']['affiliation'])
            locID.append(item['data']['locID'])
            countryOfPub.append(item['data']['countryOfPub'])
            language.append(item['data']['language'])

            grantNum.append(item['data']['grantNum'])
            fullAuthor.append(item['data']['fullAuthor'])
            meshT.append(item['data']['meshT'])
            source.append(item['data']['source'])
            fullAuthorEdited.append(item['data']['fullAuthorEdited'])

            firstAuthor.append(item['data']['firstAuthor'])
            pubYear.append(item['data']['pubYear'])
            titleAuthorStr.append(item['data']['titleAuthorStr'])
            datePulled.append(item['data']['trackingChanges'][i]['datePulled'])

            if(beforeCalculateCitationChanges == False):
                foundInGooScholar.append(item['data']['foundInGooScholar'])
                numCitations.append(item['data']['trackingChanges'][i]['numCitations'])
                levenProb.append(item['data']['levenProb'])
                fullAuthorGooScholar.append(item['data']['fullAuthorGooScholar'])
            if(beforeCalculateCitationChanges == True):
                break

        if(beforeCalculateCitationChanges == False):
            df = pd.DataFrame([pmcID, pubmedID, nlmID, journalTitle, title, creationDate, affiliation, 
                               locID, countryOfPub, language, grantNum, fullAuthor, meshT, source, 
                               fullAuthorEdited, firstAuthor, pubYear, titleAuthorStr, datePulled,
                               foundInGooScholar, numCitations, levenProb, fullAuthorGooScholar]).T
        else:
            df = pd.DataFrame([pmcID, pubmedID, nlmID, journalTitle, title, creationDate, affiliation, 
                               locID, countryOfPub, language, grantNum, fullAuthor, meshT, source, 
                               fullAuthorEdited, firstAuthor, pubYear, titleAuthorStr, datePulled]).T
        df.columns = colNames
        
        if(beforeCalculateCitationChanges == True):
            df['datePulled'] = date.datetime.now().strftime("%m-%d-%Y")
    return df

def moveItemToIgnoreContainer(key_dict: dict, pmIDList, fromContainerName: str, toContainerName: str):
    """
    Moves one or many articles from one container to another
    
    """
    fromContainer = init_cosmos(key_dict, fromContainerName)
    toContainer = init_cosmos(key_dict, toContainerName)
    for i in range(len(pmIDList)):
        pmID = pmIDList[i]
        #check if the article exists in the current container.
        checkID = getExistingIDandSearchStr(key_dict, fromContainerName)[0]
        if(str("" + pmID) in checkID):
            #move out of current container to another container
            for item in fromContainer.query_items( query = str('SELECT * FROM ' + fromContainerName), enable_cross_partition_query=True):
                if(item['id'] == str('PMID: ' + pmID)):
                    #first move to the other container
                    toContainer.upsert_item({
                            'id': item['id'],
                            'data': item['data']
                        }
                    )

                    #then delete from current container
                    fromContainer.delete_item(item, partition_key = item['id'])
                    print("" + pmID + " has been moved to the new and deleted from the old.")
        else:
            print("" + pmID + " is not in this container. Check the direction of migration.")
            
def identifyNewArticles(table, key_dict: dict):
    """
    Called in main()
    Identify new articles for a limited search. Used to search for new articles on a daily basis. 
    """
    trueArticles = getExistingIDandSearchStr(key_dict, 'pubmed')
    ignoreArticles = getExistingIDandSearchStr(key_dict, 'pubmed_ignore')
    allExistingIDs = list(np.append(trueArticles[0], ignoreArticles[0]))
    newArticles = list(set(list(table['pubmedID'])) - set(allExistingIDs))
    if(len(newArticles) > 0):
        outputTable = table[table['pubmedID'].isin(newArticles)]
        result = [outputTable, len(newArticles)]
    else:
        outputTable = pd.DataFrame()
        result = [outputTable, len(newArticles)]

    return result

def includeMissingCurrentArticles(table, key_dict: dict):
    """
    Called in main()
    For the 27 articles that were added in manually (any other manually added articles in the future), 
    we need to do add them to the total list of articles to search for. 

    """
    trueArticles = getExistingIDandSearchStr(key_dict, 'pubmed')
    ignoreArticles = getExistingIDandSearchStr(key_dict, 'pubmed_ignore')
    allExistingIDs = list(np.append(trueArticles[0], ignoreArticles[0]))
    missingArticles = list(set(allExistingIDs) - set(list(table['pubmedID'])))
    outputTable = getPMArticles(missingArticles)
    outputTable = pd.concat([outputTable, table], axis=0)
    outputTable = outputTable.reset_index()
    if ('index' in outputTable.columns):
            del outputTable['index']
            
    return outputTable

def main():
    #initialize the cosmos db dictionary
    key_dict = kv.get_key_dict()
    dateMY = "" + date.datetime.now().strftime("%m-%d-%Y")[0:2] + date.datetime.now().strftime("%m-%d-%Y")[5:10]
    secret_api_key = key_dict['SERPAPI_KEY'] #SERPAPI key
    
    #search terms/strings
    searchAll = ['ohdsi', 'omop', 'Observational Medical Outcomes Partnership Common Data Model', \
             '"Observational Medical Outcomes Partnership"', '"Observational Health Data Science and Informatics"']  
    # searchAll = addTheseArticles   #27 without relevent key words in the title/abstract/author
    
    #first search pubmed
    finalTable = getPMArticles(searchAll)
    finalTable = includeMissingCurrentArticles(finalTable, key_dict)
    finalTable = finalTable[finalTable['pubYear'] > 2010]
    numNewArticles = 0
    #check if an update has already been performed this month
    if(getTimeOfLastUpdate(key_dict)[0:2] + getTimeOfLastUpdate(key_dict)[5:10] == dateMY):
        print("Already updated this month on " + getTimeOfLastUpdate(key_dict))
        print("Identifying new articles...")
        #check if an update has already been performed today
        if(getTimeOfLastUpdate(key_dict) != str("" + date.datetime.now().strftime("%m-%d-%Y"))):
            #if not search and filter for new articles
            finalTable, numNewArticles = identifyNewArticles(finalTable, key_dict)
            #if no new articles are found. End the update/script.
            if(numNewArticles == 0):
                print("" + str(numNewArticles) + " new articles found. Update is not needed." )
            else:
                print("" + str(numNewArticles) + " new articles found. Proceed to update..." )
        else:
            print("Already checked for new articles today. Come back later:)")
    else:
        print("First update of the month.")

    #if it is the first update of the month, or if new articles have been found within the same month, upsert those articles
    if((getTimeOfLastUpdate(key_dict)[0:2] + getTimeOfLastUpdate(key_dict)[5:10] != dateMY) or (numNewArticles > 0)):
        #search google scholar and create 4 new columns
        finalTable[['foundInGooScholar', 'numCitations', 'levenProb', 'fullAuthorGooScholar']] = finalTable.apply(lambda x: getGoogleScholarCitation(x,key_dict['SERPAPI_KEY']), axis = 1, result_type='expand')
        finalTable = finalTable.reset_index()
        if ('index' in finalTable.columns):
            del finalTable['index']
            #del finalTable['level_0']

        #update the current records
        makeCSVJSON(finalTable, key_dict)
        print("Update complete.")
    else:
        print("No updates were performed.")

if __name__ == '__main__':
    main()
    #run the following line if any one or more articles need to be moved to another container. 
#     moveItemToIgnoreContainer(key_dict, ['27028034'], 'pubmed', 'pubmed_ignore')