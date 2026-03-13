import requests
import json
import os
from dotenv import load_dotenv
import time
import pymysql.cursors
import citizenphil as cp
import tmdb_functions as tf
from datetime import datetime, timedelta
import csv
import pandas as pd
import re
from SPARQLWrapper import SPARQLWrapper, SPARQLExceptions, JSON

# Load .env file 
load_dotenv()

strwikidatauseragent = os.getenv("WIKIMEDIA_USER_AGENT")
print("strwikidatauseragent",strwikidatauseragent)

strprocessesexecutedprevious = cp.f_getservervariable("strsparqlcrawlerprocessesexecuted",0)
strprocessesexecuteddesc = "List of processes executed in the Wikidata SPARQL crawler"
cp.f_setservervariable("strsparqlcrawlerprocessesexecutedprevious",strprocessesexecutedprevious,strprocessesexecuteddesc + " (previous execution)",0)
strprocessesexecuted = ""
cp.f_setservervariable("strsparqlcrawlerprocessesexecuted",strprocessesexecuted,strprocessesexecuteddesc,0)

try:
    with cp.connectioncp:
        with cp.connectioncp.cursor() as cursor:
            cursor3 = cp.connectioncp.cursor()
            # Start timing the script execution
            start_time = time.time()
            strnow = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
            cp.f_setservervariable("strsparqlcrawlerstartdatetime",strnow,"Date and time of the last start of the Wikidata SPARQL crawler",0)
            strtotalruntimedesc = "Total runtime of the Wikidata SPARQL crawler"
            strtotalruntimeprevious = cp.f_getservervariable("strsparqlcrawlertotalruntime",0)
            cp.f_setservervariable("strsparqlcrawlertotalruntimeprevious",strtotalruntimeprevious,strtotalruntimedesc + " (previous execution)",0)
            strtotalruntime = ""
            cp.f_setservervariable("strsparqlcrawlertotalruntime",strtotalruntime,strtotalruntimedesc,0)

            # Retrieving instance of values for persons (humans) used in Wikidata Sparql queries
            strsparqlpersoninstanceof = cp.f_getservervariable("strsparqlaltcrawlerpersoninstanceof",0)
            if strsparqlpersoninstanceof == "":
                strsparqlpersoninstanceof = "Q5"
                cp.f_setservervariable("strsparqlaltcrawlerpersoninstanceof",strsparqlpersoninstanceof,"Instances of values for persons (humans) used in Wikidata Sparql queries",0)
            # Retrieving instance of values for movies used in Wikidata Sparql queries
            strsparqlmovieinstanceof = cp.f_getservervariable("strsparqlaltcrawlermovieinstanceof",0)
            if strsparqlmovieinstanceof == "":
                strsparqlmovieinstanceof = "Q11424 Q202866 Q226730 Q24862 Q20650540 Q506240 Q17517379"
                cp.f_setservervariable("strsparqlaltcrawlermovieinstanceof",strsparqlmovieinstanceof,"Instances of values for movies used in Wikidata Sparql queries",0)
            # Retrieving instance of values for series used in Wikidata Sparql queries
            strsparqlserieinstanceof = cp.f_getservervariable("strsparqlaltcrawlerserieinstanceof",0)
            if strsparqlserieinstanceof == "":
                strsparqlserieinstanceof = "Q5398426 Q1259759 Q117467246 Q63952888 Q15416"
                cp.f_setservervariable("strsparqlaltcrawlerserieinstanceof",strsparqlserieinstanceof,"Instances of values for series used in Wikidata Sparql queries",0)

            #arrwikidatascope = {100: 'property'}
            #arrwikidatascope = {109: 'item'}
            #arrwikidatascope = {100: 'property', 101: 'movie', 102: 'person', 103: 'serie', 104: 'movie properties', 105: 'person properties', 106: 'movie aliases', 109: 'item'}
            #arrwikidatascope = {101: 'movie'}
            #arrwikidatascope = {101: 'movie', 104: 'movie properties', 109: 'item'}
            arrwikidatascope = {107: 'person aliases', 106: 'movie aliases', 104: 'movie properties', 105: 'person properties', 109: 'item add'}
            #arrwikidatascope = {110: 'item fix INSTANCE_OF'}
            #arrwikidatascope = {110: 'item fix INSTANCE_OF', 112: 'move item to person', 113: 'person refresh'}
            #arrwikidatascope = {111: 'cleaning'}
            arrwikidatascope = {115: 'person properties VIP', 105: 'person properties', 104: 'movie properties', 114: 'serie properties', 109: 'item add'}
            arrwikidatascope = {109: 'item add', 112: 'move item to person', 115: 'person properties VIP', 105: 'person properties', 104: 'movie properties', 114: 'serie properties'}
            if strnow <= "2026-03-18 00:00:00":
                # The item add process is executed in priority until March 10, 2026 to quickly populate the T_WC_WIKIDATA_ITEM_V1 table and then be able to use this data in the other processes
                arrwikidatascope = {112: 'move item to person', 109: 'item add'}

            for intindex,strcontent in arrwikidatascope.items():
                strcurrentprocess = f"{intindex}: processing Wikidata " + strcontent + " data using SPARQL"
                strprocessesexecuted += str(intindex) + ", "
                cp.f_setservervariable("strsparqlcrawlerprocessesexecuted",strprocessesexecuted,strprocessesexecuteddesc,0)
                print(strcurrentprocess)
                datnow = datetime.now(cp.paris_tz)
                delta30 = timedelta(days=30)
                datjminus30 = datnow - delta30
                strdatjminus30 = datjminus30.strftime("%Y-%m-%d")
                delta100 = timedelta(days=100)
                datjminus100 = datnow - delta100
                strdatjminus100 = datjminus100.strftime("%Y-%m-%d")
                if intindex == 100:
                    # Wikidata properties data download
                    cp.f_setservervariable("strsparqlcrawlerpropertiescurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL crawler",0)
                    time.sleep(90)
                    # Define the SPARQL query
                    strsparqlquery = ""
                    strsparqlquery += "SELECT ?property ?propertyLabel ?propertyDescription WHERE { "
                    strsparqlquery += "?property a wikibase:Property . "
                    strsparqlquery += "SERVICE wikibase:label {  "
                    strsparqlquery += "bd:serviceParam wikibase:language \"[AUTO_LANGUAGE],en\".  "
                    strsparqlquery += "?property rdfs:label ?propertyLabel . "
                    strsparqlquery += "?property schema:description ?propertyDescription . "
                    strsparqlquery += "} "
                    strsparqlquery += "} "
                    strsparqlquery += "ORDER BY ?property "
                    # Initialize the SPARQL wrapper
                    sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=strwikidatauseragent)
                    # Set the query and return format
                    print(strsparqlquery)
                    sparql.setQuery(strsparqlquery)
                    sparql.setReturnFormat(JSON)
                    # Execute the query and convert the results
                    try:
                        query_result = sparql.query()
                        results = query_result.convert()
                        df = pd.json_normalize(results['results']['bindings'])
                        if not df.empty:
                            for index, row in df.iterrows():
                                stritem = row['property.value']
                                # Compute strwikidataid
                                strwikidataid = ""
                                strwikidataid = stritem.split('/')[-1]
                                cp.f_setservervariable("strsparqlcrawlerpropertiescurrentvalue",strwikidataid,"Current value in the current Wikidata SPARQL crawler",0)
                                cp.f_setservervariable("strsparqlcrawlerpropertieswikidataid",strwikidataid,"Current Wikidata ID in the current Wikidata SPARQL crawler",0)
                                # Compute strlabel
                                strlabel = ""
                                if 'propertyLabel.value' in row:
                                    if row['propertyLabel.value']:
                                        if not pd.isna(row['propertyLabel.value']):
                                            strlabel = row['propertyLabel.value']
                                            # reject any label that looks like a Wikidata ID
                                            if re.match(r'^[QPL]\d+$', strlabel):
                                                strlabel = ""
                                # Compute strdescription
                                strdescription = ""
                                if 'propertyDescription.value' in row:
                                    if row['propertyDescription.value']:
                                        if not pd.isna(row['propertyDescription.value']):
                                            strdescription = row['propertyDescription.value']
                                strmessage = f"{strwikidataid} '{strlabel}' {strdescription}"
                                print(strmessage)
                                arrmoviecouples = {}
                                arrmoviecouples["ID_PROPERTY"] = strwikidataid
                                arrmoviecouples["LABEL"] = strlabel
                                arrmoviecouples["DESCRIPTION"] = strdescription
                                strsqltablename = "T_WC_WIKIDATA_PROPERTY"
                                strsqlupdatecondition = f"ID_PROPERTY = '{strwikidataid}'"
                                cp.f_sqlupdatearray(strsqltablename,arrmoviecouples,strsqlupdatecondition,1)
                    except SPARQLExceptions.EndPointInternalError as e:
                        print(f"Internal Server Error: {e}")
                    except SPARQLExceptions.QueryBadFormed as e:
                        print(f"Badly Formed Query: {e}")
                    except SPARQLExceptions.EndPointNotFound as e:
                        print(f"Endpoint Not Found: {e}")
                    except Exception as e:
                        print(f"An error occurred: {e}")
                        lngretryafter = 60
                        print(f"Rate limit exceeded. Retrying after {lngretryafter} seconds.")
                        time.sleep(lngretryafter)
                if intindex == 103:
                    # Series data download
                    lngoffset = -1
                    # Year begin for the query
                    lngyearbegin = 2028
                    #lngyearbegin = 2018
                    #lngyearbegin = 2115 for 100 years
                    #lngyearbegin = 1943
                    # Year end for the query
                    lngyearend = 1894
                    #lngyearend = 2115
                    #lngyearend = 1943
                    lngyearquery = lngyearbegin
                    intencore = True
                    strwikidataidprev = ""
                    strgenrelist = ""
                    strcolorlist = ""
                    while intencore:
                        cp.f_setservervariable("strsparqlcrawlerseriescurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL crawler",0)
                        cp.f_setservervariable("strsparqlcrawlerseriescurrentvalue",str(lngyearquery),"Current year in the Wikidata SPARQL crawler serie process",0)
                        strnow = datetime.now(cp.paris_tz).strftime("%Y%m%d-%H%M%S")
                        #time.sleep(90)
                        intencore = False
                    
                if intindex == 104:
                    # Wikidata movie properties data download
                    cp.f_setservervariable("strsparqlcrawlermoviepropertiescurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL crawler",0)
                    strsql = ""
                    strsql += "SELECT DISTINCT T_WC_TMDB_MOVIE.ID_WIKIDATA, T_WC_TMDB_MOVIE.TITLE, T_WC_TMDB_MOVIE.ORIGINAL_TITLE, T_WC_TMDB_MOVIE.DAT_RELEASE, T_WC_TMDB_MOVIE.ID_MOVIE, T_WC_TMDB_MOVIE.ID_IMDB, T_WC_IMDB_MOVIE_RATING_IMPORT.averageRating "
                    strsql += "FROM T_WC_TMDB_MOVIE "
                    #strsql += "INNER JOIN T_WC_WIKIDATA_MOVIE_V1 ON T_WC_TMDB_MOVIE.ID_WIKIDATA = T_WC_WIKIDATA_MOVIE_V1.ID_WIKIDATA "
                    strsql += "LEFT JOIN T_WC_IMDB_MOVIE_RATING_IMPORT ON T_WC_TMDB_MOVIE.ID_IMDB = T_WC_IMDB_MOVIE_RATING_IMPORT.tconst "
                    strsql += "WHERE T_WC_TMDB_MOVIE.ID_WIKIDATA IS NOT NULL AND T_WC_TMDB_MOVIE.ID_WIKIDATA <> '' "
                    strsql += "AND T_WC_TMDB_MOVIE.ID_WIKIDATA REGEXP '^Q[0-9]+$' "
                    strsql += "AND T_WC_TMDB_MOVIE.ID_WIKIDATA LIKE 'Q%' "
                    strsql += "AND (T_WC_TMDB_MOVIE.TIM_WIKIDATA_COMPLETED IS NULL OR T_WC_TMDB_MOVIE.TIM_WIKIDATA_COMPLETED < '" + strdatjminus30 + "') "
                    #strsql += "AND (T_WC_TMDB_MOVIE.ID_MOVIE IN ( "
                    #strsql += "SELECT ID_MOVIE FROM T_WC_TMDB_MOVIE_LIST WHERE ID_LIST IN ( "
                    #strsql += "SELECT ID_LIST FROM T_WC_TMDB_LIST WHERE DELETED = 0 AND USE_FOR_TAGGING >= 1 "
                    #strsql += ") "
                    #strsql += ") "
                    #strsql += "OR (T_WC_WIKIDATA_MOVIE_V1.ID_CRITERION IS NOT NULL AND T_WC_WIKIDATA_MOVIE_V1.ID_CRITERION <> 0) "
                    #strsql += ") "
                    #strsql += "AND T_WC_TMDB_MOVIE.ID_WIKIDATA = 'Q1199628' "
                    #strsql += "ORDER BY T_WC_TMDB_MOVIE.ID_MOVIE "
                    strsql += "ORDER BY T_WC_IMDB_MOVIE_RATING_IMPORT.averageRating DESC "
                    strsql += "LIMIT 1000 "
                    # strsql += "LIMIT 1 "
                    if strsql != "":
                        print(strsql)
                        #cp.f_setservervariable("strsparqlcrawlercurrentsql",strsql,"Current SQL query in the SPARQL Wikidata crawler",0)
                        cursor.execute(strsql)
                        lngrowcount = cursor.rowcount
                        print(f"{lngrowcount} lines")
                        results = cursor.fetchall()
                        for row3 in results:
                            # print("------------------------------------------")
                            strwikidataid = row3['ID_WIKIDATA']
                            lngid = row3['ID_MOVIE']
                            strmovietitle = row3['TITLE']
                            strmovieoriginaltitle = row3['ORIGINAL_TITLE']
                            datrelease = row3['DAT_RELEASE']
                            stryearrelease = ""
                            if datrelease:
                                stryearrelease = datrelease.strftime("%Y")
                            dblimdbrating = row3['averageRating']
                            #cp.f_setservervariable("strsparqlcrawlercurrentvalue",str(lngid),"Current value in the current Wikidata SPARQL crawler",0)
                            cp.f_setservervariable("strsparqlcrawlermoviepropertiescurrentvalue",str(dblimdbrating),"Current value in the current Wikidata SPARQL crawler",0)
                            cp.f_setservervariable("strsparqlcrawlermoviepropertieswikidataid",strwikidataid,"Current Wikidata ID in the current Wikidata SPARQL crawler",0)
                            strmessage = f"{lngid} {strmovietitle} ({stryearrelease})"
                            if strmovietitle != strmovieoriginaltitle:
                                strmessage += f" AKA {strmovieoriginaltitle}"
                            strmessage += f" {dblimdbrating} {strwikidataid}"
                            print(strmessage)
                            intencore = True
                            while intencore:
                                time.sleep(5)
                                # Define the SPARQL query
                                strlang = "en"
                                strsparqlquery = ""
                                strsparqlquery += "SELECT ?property ?propertyLabel ?value ?valueLabel WHERE { "
                                strsparqlquery += "  wd:" + strwikidataid + " ?p ?statement . "
                                strsparqlquery += "  ?statement ?ps ?value . "
                                strsparqlquery += "  ?property wikibase:claim ?p . "
                                strsparqlquery += "  ?property rdfs:label ?propertyLabel FILTER(LANG(?propertyLabel) = \"" + strlang + "\") . "
                                strsparqlquery += "  ?value rdfs:label ?valueLabel FILTER(LANG(?valueLabel) = \"" + strlang + "\") . "
                                strsparqlquery += "} "
                                strsparqlquery += "ORDER BY ?property ?propertyLabel "
                                # Initialize the SPARQL wrapper
                                sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=strwikidatauseragent)
                                # Set the query and return format
                                print(strsparqlquery)
                                sparql.setQuery(strsparqlquery)
                                sparql.setReturnFormat(JSON)
                                # Execute the query and convert the results
                                try:
                                    query_result = sparql.query()
                                    results = query_result.convert()
                                    intencore = False
                                    df = pd.json_normalize(results['results']['bindings'])
                                    if not df.empty:
                                        for index, row in df.iterrows():
                                            strproperty = row['property.value']
                                            # Compute strpropertyid
                                            strpropertyid = ""
                                            strpropertyid = strproperty.split('/')[-1]
                                            stritemid = ""
                                            if 'value.value' in row:
                                                if row['value.value']:
                                                    if not pd.isna(row['value.value']):
                                                        stritem = row['value.value']
                                                        stritemid = stritem.split('/')[-1]
                                            arritemcouples = {}
                                            arritemcouples["ID_WIKIDATA"] = strwikidataid
                                            arritemcouples["ID_PROPERTY"] = strpropertyid
                                            arritemcouples["ID_ITEM"] = stritemid
                                            strsqltablename = "T_WC_WIKIDATA_ITEM_PROPERTY"
                                            strsqlupdatecondition = f"ID_WIKIDATA = '{strwikidataid}' AND ID_PROPERTY = '{strpropertyid}' AND ID_ITEM = '{stritemid}'"
                                            cp.f_sqlupdatearray(strsqltablename,arritemcouples,strsqlupdatecondition,1)
                                except SPARQLExceptions.EndPointInternalError as e:
                                    print(f"Internal Server Error: {e}")
                                except SPARQLExceptions.QueryBadFormed as e:
                                    print(f"Badly Formed Query: {e}")
                                except SPARQLExceptions.EndPointNotFound as e:
                                    print(f"Endpoint Not Found: {e}")
                                except Exception as e:
                                    print(f"An error occurred: {e}")
                                    lngretryafter = 60
                                    print(f"Rate limit exceeded. Retrying after {lngretryafter} seconds.")
                                    time.sleep(lngretryafter)
                            tf.f_tmdbmoviesetwikidatacompleted(lngid)
                            
                if intindex == 114:
                    # Wikidata serie properties data download
                    cp.f_setservervariable("strsparqlcrawlerseriepropertiescurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL crawler",0)
                    strsql = ""
                    strsql += "SELECT DISTINCT T_WC_TMDB_SERIE.ID_WIKIDATA, T_WC_TMDB_SERIE.TITLE, T_WC_TMDB_SERIE.ORIGINAL_TITLE, T_WC_TMDB_SERIE.FIRST_AIR_YEAR, T_WC_TMDB_SERIE.LAST_AIR_YEAR, T_WC_TMDB_SERIE.ID_SERIE, T_WC_TMDB_SERIE.ID_IMDB, T_WC_IMDB_MOVIE_RATING_IMPORT.averageRating "
                    strsql += "FROM T_WC_TMDB_SERIE "
                    #strsql += "INNER JOIN T_WC_WIKIDATA_SERIE_V1 ON T_WC_TMDB_SERIE.ID_WIKIDATA = T_WC_WIKIDATA_SERIE_V1.ID_WIKIDATA "
                    strsql += "LEFT JOIN T_WC_IMDB_MOVIE_RATING_IMPORT ON T_WC_TMDB_SERIE.ID_IMDB = T_WC_IMDB_MOVIE_RATING_IMPORT.tconst "
                    strsql += "WHERE T_WC_TMDB_SERIE.ID_WIKIDATA IS NOT NULL AND T_WC_TMDB_SERIE.ID_WIKIDATA <> '' "
                    strsql += "AND T_WC_TMDB_SERIE.ID_WIKIDATA REGEXP '^Q[0-9]+$' "
                    strsql += "AND T_WC_TMDB_SERIE.ID_WIKIDATA LIKE 'Q%' "
                    strsql += "AND (T_WC_TMDB_SERIE.TIM_WIKIDATA_COMPLETED IS NULL OR T_WC_TMDB_SERIE.TIM_WIKIDATA_COMPLETED < '" + strdatjminus30 + "') "
                    #strsql += "AND (T_WC_TMDB_SERIE.ID_SERIE IN ( "
                    #strsql += "SELECT ID_SERIE FROM T_WC_TMDB_SERIE_LIST WHERE ID_LIST IN ( "
                    #strsql += "SELECT ID_LIST FROM T_WC_TMDB_LIST WHERE DELETED = 0 AND USE_FOR_TAGGING >= 1 "
                    #strsql += ") "
                    #strsql += ") "
                    #strsql += "OR (T_WC_WIKIDATA_MOVIE_V1.ID_CRITERION IS NOT NULL AND T_WC_WIKIDATA_MOVIE_V1.ID_CRITERION <> 0) "
                    #strsql += ") "
                    #strsql += "AND T_WC_TMDB_MOVIE.ID_WIKIDATA = 'Q1199628' "
                    #strsql += "ORDER BY T_WC_TMDB_MOVIE.ID_MOVIE "
                    strsql += "ORDER BY T_WC_IMDB_MOVIE_RATING_IMPORT.averageRating DESC "
                    strsql += "LIMIT 1000 "
                    # strsql += "LIMIT 1 "
                    if strsql != "":
                        print(strsql)
                        #cp.f_setservervariable("strsparqlcrawlercurrentsql",strsql,"Current SQL query in the SPARQL Wikidata crawler",0)
                        cursor.execute(strsql)
                        lngrowcount = cursor.rowcount
                        print(f"{lngrowcount} lines")
                        results = cursor.fetchall()
                        for row3 in results:
                            # print("------------------------------------------")
                            strwikidataid = row3['ID_WIKIDATA']
                            lngid = row3['ID_SERIE']
                            strserieoriginaltitle = row3['TITLE']
                            strserieoriginaltitle = row3['ORIGINAL_TITLE']
                            strfirstairyear = row3['FIRST_AIR_YEAR']
                            strlastairyear = row3['LAST_AIR_YEAR']
                            dblimdbrating = row3['averageRating']
                            #cp.f_setservervariable("strsparqlcrawlercurrentvalue",str(lngid),"Current value in the current Wikidata SPARQL crawler",0)
                            cp.f_setservervariable("strsparqlcrawlerseriepropertiescurrentvalue",str(dblimdbrating),"Current value in the current Wikidata SPARQL crawler",0)
                            cp.f_setservervariable("strsparqlcrawlerseriepropertieswikidataid",strwikidataid,"Current Wikidata ID in the current Wikidata SPARQL crawler",0)
                            strmessage = f"{lngid} {strserieoriginaltitle} ({strfirstairyear}-{strlastairyear})"
                            if strserieoriginaltitle != strserieoriginaltitle:
                                strmessage += f" AKA {strserieoriginaltitle}"
                            strmessage += f" {dblimdbrating} {strwikidataid}"
                            print(strmessage)
                            intencore = True
                            while intencore:
                                time.sleep(5)
                                # Define the SPARQL query
                                strlang = "en"
                                strsparqlquery = ""
                                strsparqlquery += "SELECT ?property ?propertyLabel ?value ?valueLabel WHERE { "
                                strsparqlquery += "  wd:" + strwikidataid + " ?p ?statement . "
                                strsparqlquery += "  ?statement ?ps ?value . "
                                strsparqlquery += "  ?property wikibase:claim ?p . "
                                strsparqlquery += "  ?property rdfs:label ?propertyLabel FILTER(LANG(?propertyLabel) = \"" + strlang + "\") . "
                                strsparqlquery += "  ?value rdfs:label ?valueLabel FILTER(LANG(?valueLabel) = \"" + strlang + "\") . "
                                strsparqlquery += "} "
                                strsparqlquery += "ORDER BY ?property ?propertyLabel "
                                # Initialize the SPARQL wrapper
                                sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=strwikidatauseragent)
                                # Set the query and return format
                                print(strsparqlquery)
                                sparql.setQuery(strsparqlquery)
                                sparql.setReturnFormat(JSON)
                                # Execute the query and convert the results
                                try:
                                    query_result = sparql.query()
                                    results = query_result.convert()
                                    intencore = False
                                    df = pd.json_normalize(results['results']['bindings'])
                                    if not df.empty:
                                        for index, row in df.iterrows():
                                            strproperty = row['property.value']
                                            # Compute strpropertyid
                                            strpropertyid = ""
                                            strpropertyid = strproperty.split('/')[-1]
                                            stritemid = ""
                                            if 'value.value' in row:
                                                if row['value.value']:
                                                    if not pd.isna(row['value.value']):
                                                        stritem = row['value.value']
                                                        stritemid = stritem.split('/')[-1]
                                            arritemcouples = {}
                                            arritemcouples["ID_WIKIDATA"] = strwikidataid
                                            arritemcouples["ID_PROPERTY"] = strpropertyid
                                            arritemcouples["ID_ITEM"] = stritemid
                                            strsqltablename = "T_WC_WIKIDATA_ITEM_PROPERTY"
                                            strsqlupdatecondition = f"ID_WIKIDATA = '{strwikidataid}' AND ID_PROPERTY = '{strpropertyid}' AND ID_ITEM = '{stritemid}'"
                                            cp.f_sqlupdatearray(strsqltablename,arritemcouples,strsqlupdatecondition,1)
                                except SPARQLExceptions.EndPointInternalError as e:
                                    print(f"Internal Server Error: {e}")
                                except SPARQLExceptions.QueryBadFormed as e:
                                    print(f"Badly Formed Query: {e}")
                                except SPARQLExceptions.EndPointNotFound as e:
                                    print(f"Endpoint Not Found: {e}")
                                except Exception as e:
                                    print(f"An error occurred: {e}")
                                    lngretryafter = 60
                                    print(f"Rate limit exceeded. Retrying after {lngretryafter} seconds.")
                                    time.sleep(lngretryafter)
                            tf.f_tmdbseriesetwikidatacompleted(lngid)
                            
                if intindex == 105 or intindex == 115:
                    # Wikidata person properties data download
                    cp.f_setservervariable("strsparqlcrawlerpersonpropertiescurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL crawler",0)
                    if intindex == 115:
                        # Enable the following SQL query to update all persons with VIP status 
                        strsql = f"""SELECT DISTINCT T_WC_TMDB_PERSON.ID_WIKIDATA, T_WC_TMDB_PERSON.NAME, T_WC_TMDB_PERSON.ID_PERSON, T_WC_TMDB_PERSON.POPULARITY 
                        FROM T_WC_TMDB_PERSON 
                        INNER JOIN T_WC_TMDB_PERSON_SEARCH ON T_WC_TMDB_PERSON.ID_PERSON = T_WC_TMDB_PERSON_SEARCH.ID_PERSON 
WHERE T_WC_TMDB_PERSON.ID_WIKIDATA IS NOT NULL AND T_WC_TMDB_PERSON.ID_WIKIDATA <> '' 
AND T_WC_TMDB_PERSON.ID_WIKIDATA REGEXP '^Q[0-9]+$'
AND T_WC_TMDB_PERSON.ID_WIKIDATA LIKE 'Q%' 
AND (T_WC_TMDB_PERSON.TIM_WIKIDATA_COMPLETED IS NULL OR T_WC_TMDB_PERSON.TIM_WIKIDATA_COMPLETED < '{strdatjminus100}') 
ORDER BY T_WC_TMDB_PERSON.ID_PERSON ASC 
"""
                    else:
                        # Regular update of persons
                        strsql = ""
                        strsql += "SELECT DISTINCT T_WC_TMDB_PERSON.ID_WIKIDATA, T_WC_TMDB_PERSON.NAME, T_WC_TMDB_PERSON.ID_PERSON, T_WC_TMDB_PERSON.POPULARITY "
                        strsql += "FROM T_WC_TMDB_PERSON "
                        #strsql += "INNER JOIN T_WC_WIKIDATA_PERSON_V1 ON T_WC_TMDB_PERSON.ID_WIKIDATA = T_WC_WIKIDATA_PERSON_V1.ID_WIKIDATA "
                        strsql += "WHERE T_WC_TMDB_PERSON.ID_WIKIDATA IS NOT NULL AND T_WC_TMDB_PERSON.ID_WIKIDATA <> '' "
                        strsql += "AND T_WC_TMDB_PERSON.ID_WIKIDATA REGEXP '^Q[0-9]+$' "
                        strsql += "AND T_WC_TMDB_PERSON.ID_WIKIDATA LIKE 'Q%' "
                        strsql += "AND (T_WC_TMDB_PERSON.TIM_WIKIDATA_COMPLETED IS NULL OR T_WC_TMDB_PERSON.TIM_WIKIDATA_COMPLETED < '" + strdatjminus30 + "') "
                        strsql += "ORDER BY T_WC_TMDB_PERSON.POPULARITY DESC "
                        strsql += "LIMIT 2000 "
                        # strsql += "LIMIT 1 "
                    if strsql != "":
                        print(strsql)
                        cursor.execute(strsql)
                        lngrowcount = cursor.rowcount
                        print(f"{lngrowcount} lines")
                        results = cursor.fetchall()
                        for row3 in results:
                            # print("------------------------------------------")
                            strwikidataid = row3['ID_WIKIDATA']
                            lngid = row3['ID_PERSON']
                            strpersonname = row3['NAME']
                            dblpersonpopularity = row3['POPULARITY']
                            cp.f_setservervariable("strsparqlcrawlerpersonspropertiescurrentvalue",str(dblpersonpopularity),"Current value in the current Wikidata SPARQL crawler",0)
                            cp.f_setservervariable("strsparqlcrawlerpersonspropertieswikidataid",strwikidataid,"Current Wikidata ID in the current Wikidata SPARQL crawler",0)
                            strmessage = f"{lngid} {strpersonname}"
                            strmessage += f" {dblpersonpopularity} {strwikidataid}"
                            print(strmessage)
                            intencore = True
                            while intencore:
                                time.sleep(5)
                                # Define the SPARQL query
                                strlang = "en"
                                strsparqlquery = ""
                                strsparqlquery += "SELECT ?property ?propertyLabel ?value ?valueLabel WHERE { "
                                strsparqlquery += "  wd:" + strwikidataid + " ?p ?statement . "
                                strsparqlquery += "  ?statement ?ps ?value . "
                                strsparqlquery += "  ?property wikibase:claim ?p . "
                                strsparqlquery += "  ?property rdfs:label ?propertyLabel FILTER(LANG(?propertyLabel) = \"" + strlang + "\") . "
                                strsparqlquery += "  ?value rdfs:label ?valueLabel FILTER(LANG(?valueLabel) = \"" + strlang + "\") . "
                                strsparqlquery += "} "
                                strsparqlquery += "ORDER BY ?property ?propertyLabel "
                                # Initialize the SPARQL wrapper
                                sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=strwikidatauseragent)
                                # Set the query and return format
                                print(strsparqlquery)
                                sparql.setQuery(strsparqlquery)
                                sparql.setReturnFormat(JSON)
                                # Execute the query and convert the results
                                try:
                                    query_result = sparql.query()
                                    results = query_result.convert()
                                    intencore = False
                                    df = pd.json_normalize(results['results']['bindings'])
                                    if not df.empty:
                                        for index, row in df.iterrows():
                                            strproperty = row['property.value']
                                            #print("strproperty",strproperty)
                                            # Compute strpropertyid
                                            strpropertyid = ""
                                            strpropertyid = strproperty.split('/')[-1]
                                            #print("strpropertyid",strpropertyid)
                                            stritemid = ""
                                            if 'value.value' in row:
                                                if row['value.value']:
                                                    if not pd.isna(row['value.value']):
                                                        stritem = row['value.value']
                                                        stritemid = stritem.split('/')[-1]
                                                        #print("stritemid",stritemid)
                                            arritemcouples = {}
                                            arritemcouples["ID_WIKIDATA"] = strwikidataid
                                            arritemcouples["ID_PROPERTY"] = strpropertyid
                                            arritemcouples["ID_ITEM"] = stritemid
                                            strsqltablename = "T_WC_WIKIDATA_ITEM_PROPERTY"
                                            strsqlupdatecondition = f"ID_WIKIDATA = '{strwikidataid}' AND ID_PROPERTY = '{strpropertyid}' AND ID_ITEM = '{stritemid}'"
                                            #print("strsqlupdatecondition",strsqlupdatecondition)
                                            cp.f_sqlupdatearray(strsqltablename,arritemcouples,strsqlupdatecondition,1)
                                except SPARQLExceptions.EndPointInternalError as e:
                                    print(f"Internal Server Error: {e}")
                                except SPARQLExceptions.QueryBadFormed as e:
                                    print(f"Badly Formed Query: {e}")
                                except SPARQLExceptions.EndPointNotFound as e:
                                    print(f"Endpoint Not Found: {e}")
                                except Exception as e:
                                    print(f"An error occurred: {e}")
                                    lngretryafter = 60
                                    print(f"Rate limit exceeded. Retrying after {lngretryafter} seconds.")
                                    time.sleep(lngretryafter)
                            tf.f_tmdbpersonsetwikidatacompleted(lngid)
                if intindex == 106:
                    # Wikidata movie aliases data download
                    cp.f_setservervariable("strsparqlcrawlermoviealiasescurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL crawler",0)
                    strsql = ""
                    strsql += "SELECT DISTINCT T_WC_TMDB_MOVIE.ID_WIKIDATA, T_WC_TMDB_MOVIE.TITLE, T_WC_TMDB_MOVIE.ORIGINAL_TITLE, T_WC_TMDB_MOVIE.DAT_RELEASE, T_WC_TMDB_MOVIE.ID_MOVIE, T_WC_TMDB_MOVIE.ID_IMDB, T_WC_IMDB_MOVIE_RATING_IMPORT.averageRating "
                    strsql += "FROM T_WC_TMDB_MOVIE "
                    strsql += "INNER JOIN T_WC_WIKIDATA_MOVIE_V1 ON T_WC_TMDB_MOVIE.ID_WIKIDATA = T_WC_WIKIDATA_MOVIE_V1.ID_WIKIDATA "
                    strsql += "LEFT JOIN T_WC_IMDB_MOVIE_RATING_IMPORT ON T_WC_TMDB_MOVIE.ID_IMDB = T_WC_IMDB_MOVIE_RATING_IMPORT.tconst "
                    strsql += "WHERE T_WC_TMDB_MOVIE.ID_WIKIDATA IS NOT NULL AND T_WC_TMDB_MOVIE.ID_WIKIDATA <> '' "
                    strsql += "AND T_WC_TMDB_MOVIE.ID_WIKIDATA REGEXP '^Q[0-9]+$' "
                    strsql += "AND T_WC_TMDB_MOVIE.ID_WIKIDATA LIKE 'Q%' "
                    strsql += "AND T_WC_WIKIDATA_MOVIE_V1.ALIASES IS NULL "
                    #strsql += "AND (T_WC_TMDB_MOVIE.ID_MOVIE IN ( "
                    #strsql += "SELECT ID_MOVIE FROM T_WC_TMDB_MOVIE_LIST WHERE ID_LIST IN ( "
                    #strsql += "SELECT ID_LIST FROM T_WC_TMDB_LIST WHERE DELETED = 0 AND USE_FOR_TAGGING >= 1 "
                    #strsql += ") "
                    #strsql += ") "
                    #strsql += "OR (T_WC_WIKIDATA_MOVIE_V1.ID_CRITERION IS NOT NULL AND T_WC_WIKIDATA_MOVIE_V1.ID_CRITERION <> 0) "
                    #strsql += ") "
                    strsql += "ORDER BY T_WC_IMDB_MOVIE_RATING_IMPORT.averageRating DESC "
                    strsql += "LIMIT 500 "
                    #strsql += "LIMIT 5 "
                    if strsql != "":
                        print(strsql)
                        cursor.execute(strsql)
                        lngrowcount = cursor.rowcount
                        print(f"{lngrowcount} lines")
                        results = cursor.fetchall()
                        for row3 in results:
                            # print("------------------------------------------")
                            strwikidataid = row3['ID_WIKIDATA']
                            lngid = row3['ID_MOVIE']
                            strmovietitle = row3['TITLE']
                            strmovieoriginaltitle = row3['ORIGINAL_TITLE']
                            datrelease = row3['DAT_RELEASE']
                            stryearrelease = ""
                            if datrelease:
                                stryearrelease = datrelease.strftime("%Y")
                            dblimdbrating = row3['averageRating']
                            cp.f_setservervariable("strsparqlcrawlermoviealiasescurrentvalue",str(dblimdbrating),"Current value in the current Wikidata SPARQL crawler",0)
                            cp.f_setservervariable("strsparqlcrawlermoviealiaseswikidataid",strwikidataid,"Current Wikidata ID in the current Wikidata SPARQL crawler",0)
                            strmessage = f"{lngid} {strmovietitle} ({stryearrelease})"
                            if strmovietitle != strmovieoriginaltitle:
                                strmessage += f" AKA {strmovieoriginaltitle}"
                            strmessage += f" {dblimdbrating} {strwikidataid}"
                            print(strmessage)
                            intencore = True
                            while intencore:
                                time.sleep(5)
                                # Define the SPARQL query
                                strsparqlquery = ""
                                strsparqlquery += "SELECT ?alias WHERE { "
                                strsparqlquery += "  wd:" + strwikidataid + " skos:altLabel ?alias. "
                                strsparqlquery += "  FILTER(LANG(?alias) IN (\"en\", \"fr\")) "
                                strsparqlquery += "} "
                                # Initialize the SPARQL wrapper
                                sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=strwikidatauseragent)
                                # Set the query and return format
                                print(strsparqlquery)
                                sparql.setQuery(strsparqlquery)
                                sparql.setReturnFormat(JSON)
                                # Execute the query and convert the results
                                try:
                                    query_result = sparql.query()
                                    results = query_result.convert()
                                    intencore = False
                                    strmoviealiases = "|"
                                    df = pd.json_normalize(results['results']['bindings'])
                                    if not df.empty:
                                        for index, row in df.iterrows():
                                            stralias = row['alias.value']
                                            if stralias != "":
                                                straliassearch = "|" + stralias + "|"
                                                if straliassearch not in strmoviealiases:
                                                    strmoviealiases += stralias + "|"
                                    arritemcouples = {}
                                    arritemcouples["ID_WIKIDATA"] = strwikidataid
                                    arritemcouples["ALIASES"] = strmoviealiases
                                    strsqltablename = "T_WC_WIKIDATA_MOVIE_V1"
                                    strsqlupdatecondition = f"ID_WIKIDATA = '{strwikidataid}'"
                                    cp.f_sqlupdatearray(strsqltablename,arritemcouples,strsqlupdatecondition,1)
                                except SPARQLExceptions.EndPointInternalError as e:
                                    print(f"Internal Server Error: {e}")
                                except SPARQLExceptions.QueryBadFormed as e:
                                    print(f"Badly Formed Query: {e}")
                                except SPARQLExceptions.EndPointNotFound as e:
                                    print(f"Endpoint Not Found: {e}")
                                except Exception as e:
                                    print(f"An error occurred: {e}")
                                    lngretryafter = 60
                                    print(f"Rate limit exceeded. Retrying after {lngretryafter} seconds.")
                                    time.sleep(lngretryafter)
                if intindex == 107:
                    # Wikidata person aliases data download
                    cp.f_setservervariable("strsparqlcrawlerpersonaliasescurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL crawler",0)
                    strsql = ""
                    strsql += "SELECT DISTINCT T_WC_TMDB_PERSON.ID_WIKIDATA, T_WC_TMDB_PERSON.NAME, T_WC_TMDB_PERSON.ID_PERSON, T_WC_TMDB_PERSON.POPULARITY "
                    strsql += "FROM T_WC_TMDB_PERSON "
                    strsql += "INNER JOIN T_WC_WIKIDATA_PERSON_V1 ON T_WC_TMDB_PERSON.ID_WIKIDATA = T_WC_WIKIDATA_PERSON_V1.ID_WIKIDATA "
                    strsql += "WHERE T_WC_TMDB_PERSON.ID_WIKIDATA IS NOT NULL AND T_WC_TMDB_PERSON.ID_WIKIDATA <> '' "
                    strsql += "AND T_WC_TMDB_PERSON.ID_WIKIDATA REGEXP '^Q[0-9]+$' "
                    strsql += "AND T_WC_TMDB_PERSON.ID_WIKIDATA LIKE 'Q%' "
                    strsql += "AND T_WC_WIKIDATA_PERSON_V1.ALIASES IS NULL "
                    #strsql += "AND T_WC_TMDB_PERSON.ID_PERSON = 3829 "
                    strsql += "ORDER BY T_WC_TMDB_PERSON.POPULARITY DESC "
                    strsql += "LIMIT 1000 "
                    #strsql += "LIMIT 5 "
                    if strsql != "":
                        print(strsql)
                        cursor.execute(strsql)
                        lngrowcount = cursor.rowcount
                        print(f"{lngrowcount} lines")
                        results = cursor.fetchall()
                        for row3 in results:
                            # print("------------------------------------------")
                            strwikidataid = row3['ID_WIKIDATA']
                            lngid = row3['ID_PERSON']
                            strpersonname = row3['NAME']
                            dblpersonpopularity = row3['POPULARITY']
                            cp.f_setservervariable("strsparqlcrawlerpersonaliasescurrentvalue",str(dblpersonpopularity),"Current value in the current Wikidata SPARQL crawler",0)
                            cp.f_setservervariable("strsparqlcrawlerpersonaliaseswikidataid",strwikidataid,"Current Wikidata ID in the current Wikidata SPARQL crawler",0)
                            strmessage = f"{lngid} {strpersonname}"
                            strmessage += f" {strwikidataid}"
                            print(strmessage)
                            intencore = True
                            while intencore:
                                time.sleep(5)
                                # Define the SPARQL query
                                strsparqlquery = ""
                                strsparqlquery += "SELECT ?alias WHERE { "
                                strsparqlquery += "  wd:" + strwikidataid + " skos:altLabel ?alias. "
                                strsparqlquery += "  FILTER(LANG(?alias) IN (\"en\", \"fr\")) "
                                strsparqlquery += "} "
                                # Initialize the SPARQL wrapper
                                sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=strwikidatauseragent)
                                # Set the query and return format
                                print(strsparqlquery)
                                sparql.setQuery(strsparqlquery)
                                sparql.setReturnFormat(JSON)
                                # Execute the query and convert the results
                                try:
                                    query_result = sparql.query()
                                    results = query_result.convert()
                                    intencore = False
                                    strpersonaliases = "|"
                                    df = pd.json_normalize(results['results']['bindings'])
                                    if not df.empty:
                                        for index, row in df.iterrows():
                                            stralias = row['alias.value']
                                            if stralias != "":
                                                straliassearch = "|" + stralias + "|"
                                                if straliassearch not in strpersonaliases:
                                                    strpersonaliases += stralias + "|"
                                    arritemcouples = {}
                                    arritemcouples["ID_WIKIDATA"] = strwikidataid
                                    arritemcouples["ALIASES"] = strpersonaliases
                                    strsqltablename = "T_WC_WIKIDATA_PERSON_V1"
                                    strsqlupdatecondition = f"ID_WIKIDATA = '{strwikidataid}'"
                                    cp.f_sqlupdatearray(strsqltablename,arritemcouples,strsqlupdatecondition,1)
                                except SPARQLExceptions.EndPointInternalError as e:
                                    print(f"Internal Server Error: {e}")
                                except SPARQLExceptions.QueryBadFormed as e:
                                    print(f"Badly Formed Query: {e}")
                                except SPARQLExceptions.EndPointNotFound as e:
                                    print(f"Endpoint Not Found: {e}")
                                except Exception as e:
                                    print(f"An error occurred: {e}")
                                    lngretryafter = 60
                                    print(f"Rate limit exceeded. Retrying after {lngretryafter} seconds.")
                                    time.sleep(lngretryafter)
                if intindex == 109:
                    # Wikidata items data download, new (109)
                    cp.f_setservervariable("strsparqlcrawleritemscurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL crawler",0)
                    strwikidataidold = cp.f_getservervariable("strsparqlcrawleritemswikidataid",0)
                    rows_to_process = 5000
                    strsql = ""
                    strsql += "SELECT DISTINCT ID_ITEM "
                    strsql += "FROM T_WC_WIKIDATA_ITEM_PROPERTY "
                    strsql += "WHERE ID_ITEM LIKE 'Q%' "
                    if strwikidataidold != "":
                        strsql += f"AND ID_ITEM > '{strwikidataidold}' "
                    strsql += "AND ID_ITEM NOT IN (SELECT ID_WIKIDATA FROM T_WC_WIKIDATA_ITEM_V1 WHERE LABEL <> '' AND LABEL IS NOT NULL) "
                    strsql += "AND ID_ITEM NOT IN (SELECT ID_WIKIDATA FROM T_WC_WIKIDATA_PERSON_V1 WHERE NAME <> '' AND NAME IS NOT NULL) "
                    strsql += "AND ID_ITEM NOT IN (SELECT ID_WIKIDATA FROM T_WC_WIKIDATA_MOVIE_V1 WHERE TITLE <> '' AND TITLE IS NOT NULL) "
                    strsql += "AND ID_ITEM NOT IN (SELECT ID_WIKIDATA FROM T_WC_WIKIDATA_SERIE_V1 WHERE TITLE <> '' AND TITLE IS NOT NULL) "
                    strsql += "ORDER BY ID_ITEM "
                    strsql += f"LIMIT {rows_to_process} "
                    # strsql += "LIMIT 1 "
                    if strsql != "":
                        print(strsql)
                        cursor.execute(strsql)
                        lngrowcount = cursor.rowcount
                        print(f"{lngrowcount} lines")
                        results = cursor.fetchall()
                        for row3 in results:
                            strwikidataid = row3['ID_ITEM']
                            cp.f_setservervariable("strsparqlcrawleritemscurrentvalue",strwikidataid,"Current value in the current Wikidata SPARQL crawler",0)
                            cp.f_setservervariable("strsparqlcrawleritemswikidataid",strwikidataid,"Current Wikidata ID in the current Wikidata SPARQL crawler",0)
                            #print(f"{strdesc} id: {strwikidataid}")
                            arrlang = {1: 'en', 2: 'fr'}
                            for intlang, strlang in arrlang.items():
                                if strlang == "en":
                                    strlangfull = "en,de,es,it,pt,ru,zh,ja,fr,nl,sv,pl,fi,cs,hu,da,ro,ko,ar,he,el,vi,th,uk,ca,eo,gl,la,li,lt,ms,nn,oc,or,ps,qu,sa,sc,sr,sw,tg,tk,tl,tt,ug,ve,wuu,xh,yor,zul"
                                elif strlang == "fr":
                                    strlangfull = "fr,en,de,es,it,pt,ru,zh,ja"
                                intencore = True
                                while intencore:
                                    time.sleep(5)
                                    # Define the SPARQL query
                                    strsparqlquery = ""
                                    strsparqlquery += "SELECT ?item ?itemLabel ?itemDescription ?itemAlias ?instanceOf WHERE { "
                                    strsparqlquery += "VALUES ?item { wd:" + strwikidataid + " } "
                                    strsparqlquery += "OPTIONAL { ?item wdt:P31 ?instanceOf } "
                                    strsparqlquery += "SERVICE wikibase:label {  "
                                    strsparqlquery += "bd:serviceParam wikibase:language \"" + strlangfull + "\". "
                                    strsparqlquery += "} "
                                    strsparqlquery += "OPTIONAL { ?item skos:altLabel ?itemAlias. FILTER (LANG(?itemAlias) = \"" + strlang + "\") } "
                                    strsparqlquery += "} "
                                    # Initialize the SPARQL wrapper
                                    sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=strwikidatauseragent)
                                    # Set the query and return format
                                    print(strsparqlquery)
                                    sparql.setQuery(strsparqlquery)
                                    sparql.setReturnFormat(JSON)
                                    # Execute the query and convert the results
                                    try:
                                        query_result = sparql.query()
                                        results = query_result.convert()
                                        intencore = False
                                        df = pd.json_normalize(results['results']['bindings'])
                                        if not df.empty:
                                            strlabel = ""
                                            strdescription = ""
                                            straliases = ""
                                            strinstanceof = ""
                                            strinstanceofid = ""
                                            for index, row in df.iterrows():
                                                if strlabel == "":
                                                    if 'itemLabel.value' in row:
                                                        if row['itemLabel.value']:
                                                            if not pd.isna(row['itemLabel.value']):
                                                                strlabel = row['itemLabel.value']
                                                                # reject any label that looks like a Wikidata ID
                                                                if re.match(r'^[QPL]\d+$', strlabel):
                                                                    strlabel = ""
                                                if strdescription == "":
                                                    if 'itemDescription.value' in row:
                                                        if row['itemDescription.value']:
                                                            if not pd.isna(row['itemDescription.value']):
                                                                strdescription = row['itemDescription.value']
                                                if 'itemAlias.value' in row:
                                                    if row['itemAlias.value']:
                                                        if not pd.isna(row['itemAlias.value']):
                                                            stralias = row['itemAlias.value']
                                                            if stralias != "":
                                                                if straliases == "":
                                                                    straliases = "|"
                                                                straliases += stralias + "|"
                                                if 'instanceOf.value' in row:
                                                    if row['instanceOf.value']:
                                                        if not pd.isna(row['instanceOf.value']):
                                                            strinstanceof = row['instanceOf.value']
                                                            strinstanceofid = strinstanceof.split('/')[-1]
                                            arritemcouples = {}
                                            arritemcouples["ID_WIKIDATA"] = strwikidataid
                                            arritemcouples["LANG"] = strlang
                                            arritemcouples["LABEL"] = strlabel
                                            arritemcouples["DESCRIPTION"] = strdescription
                                            arritemcouples["ALIASES"] = straliases
                                            arritemcouples["INSTANCE_OF"] = strinstanceofid
                                            strsqltablename = "T_WC_WIKIDATA_ITEM_V1"
                                            strsqlupdatecondition = f"ID_WIKIDATA = '{strwikidataid}' AND LANG = '{strlang}'"
                                            cp.f_sqlupdatearray(strsqltablename,arritemcouples,strsqlupdatecondition,1)
                                    except SPARQLExceptions.EndPointInternalError as e:
                                        print(f"Internal Server Error: {e}")
                                    except SPARQLExceptions.QueryBadFormed as e:
                                        print(f"Badly Formed Query: {e}")
                                    except SPARQLExceptions.EndPointNotFound as e:
                                        print(f"Endpoint Not Found: {e}")
                                    except Exception as e:
                                        print(f"An error occurred: {e}")
                                        lngretryafter = 60
                                        print(f"Rate limit exceeded. Retrying after {lngretryafter} seconds.")
                                        time.sleep(lngretryafter)
                        if lngrowcount < rows_to_process:
                            # We finished crawling all items, we can reset the last Wikidata ID to process
                            cp.f_setservervariable("strsparqlcrawleritemswikidataid","", "Current Wikidata ID in the current Wikidata SPARQL crawler",0)
                if intindex == 110:
                    # Wikidata items data download, fix INSTANCE_OF (110)
                    cp.f_setservervariable("strsparqlcrawleritemscurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL crawler",0)
                    strsql = ""
                    strsql += "SELECT DISTINCT ID_WIKIDATA FROM T_WC_WIKIDATA_ITEM_V1 "
                    strsql += "WHERE INSTANCE_OF IS NULL "
                    strsql += "ORDER BY TIM_UPDATED ASC "
                    #strsql += "LIMIT 100 "
                    # strsql += "LIMIT 1 "
                    if strsql != "":
                        print(strsql)
                        cursor.execute(strsql)
                        lngrowcount = cursor.rowcount
                        print(f"{lngrowcount} lines")
                        results = cursor.fetchall()
                        strwikidataidall = ""
                        lngitemcount = 0
                        for row3 in results:
                            strwikidataid = row3['ID_WIKIDATA']
                            strwikidataidall += " wd:" + strwikidataid
                            lngitemcount += 1
                            if lngitemcount >= 100:
                                print(strwikidataidall)
                                intencore = True
                                while intencore:
                                    time.sleep(5)
                                    # Define the SPARQL query
                                    strsparqlquery = ""
                                    strsparqlquery += "SELECT ?item ?instanceOf WHERE { "
                                    strsparqlquery += "VALUES ?item { " + strwikidataidall + " } "
                                    strsparqlquery += "?item wdt:P31 ?instanceOf. "
                                    strsparqlquery += "} "
                                    # Initialize the SPARQL wrapper
                                    sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=strwikidatauseragent)
                                    # Set the query and return format
                                    print(strsparqlquery)
                                    sparql.setQuery(strsparqlquery)
                                    sparql.setReturnFormat(JSON)
                                    # Execute the query and convert the results
                                    try:
                                        query_result = sparql.query()
                                        results = query_result.convert()
                                        intencore = False
                                        df = pd.json_normalize(results['results']['bindings'])
                                        if not df.empty:
                                            for index, row in df.iterrows():
                                                stritem = row['item.value']
                                                # Compute strwikidataid
                                                strwikidataid = ""
                                                strwikidataid = stritem.split('/')[-1]
                                                cp.f_setservervariable("strsparqlcrawleritemfixinstanceofcurrentvalue",strwikidataid,"Current value in the current Wikidata SPARQL crawler",0)
                                                cp.f_setservervariable("strsparqlcrawleritemfixinstanceofwikidataid",strwikidataid,"Current Wikidata ID in the current Wikidata SPARQL crawler",0)
                                                strinstanceof = ""
                                                strinstanceofid = ""
                                                if 'instanceOf.value' in row:
                                                    if row['instanceOf.value']:
                                                        if not pd.isna(row['instanceOf.value']):
                                                            strinstanceof = row['instanceOf.value']
                                                            strinstanceofid = strinstanceof.split('/')[-1]
                                                arritemcouples = {}
                                                arritemcouples["ID_WIKIDATA"] = strwikidataid
                                                arritemcouples["INSTANCE_OF"] = strinstanceofid
                                                strsqltablename = "T_WC_WIKIDATA_ITEM_V1"
                                                strsqlupdatecondition = f"ID_WIKIDATA = '{strwikidataid}' "
                                                cp.f_sqlupdatearray(strsqltablename,arritemcouples,strsqlupdatecondition,1)
                                                strwikidataidall = ""
                                                lngitemcount = 0
                                    except SPARQLExceptions.EndPointInternalError as e:
                                        print(f"Internal Server Error: {e}")
                                    except SPARQLExceptions.QueryBadFormed as e:
                                        print(f"Badly Formed Query: {e}")
                                    except SPARQLExceptions.EndPointNotFound as e:
                                        print(f"Endpoint Not Found: {e}")
                                    except Exception as e:
                                        print(f"An error occurred: {e}")
                                        lngretryafter = 60
                                        print(f"Rate limit exceeded. Retrying after {lngretryafter} seconds.")
                                        time.sleep(lngretryafter)
                if intindex == 112:
                    # Wikidata move items to person when INSTANCE_OF is Q5
                    cp.f_setservervariable("strsparqlcrawleritemscurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL crawler",0)
                    strsql = ""
                    strsql += "SELECT * FROM T_WC_WIKIDATA_ITEM_V1 "
                    arrinstanceof = [v for v in strsparqlpersoninstanceof.split() if v]
                    if not arrinstanceof:
                        arrinstanceof = ["Q5"]
                    strsqlinstanceof = ",".join([f"'{v}'" for v in arrinstanceof])
                    strsql += f"WHERE INSTANCE_OF IN ({strsqlinstanceof}) "
                    strsql += "AND LANG = 'en' "
                    #strsql += "AND ID_WIKIDATA NOT IN (SELECT ID_WIKIDATA FROM T_WC_WIKIDATA_PERSON_V1) "
                    strsql += "ORDER BY TIM_UPDATED ASC "
                    #strsql += "LIMIT 1 "
                    #strsql += "LIMIT 1000 "
                    if strsql != "":
                        print(strsql)
                        cursor.execute(strsql)
                        lngrowcount = cursor.rowcount
                        print(f"{lngrowcount} lines")
                        results = cursor.fetchall()
                        for row3 in results:
                            strwikidataid = row3['ID_WIKIDATA']
                            strname = row3['LABEL']
                            straliases = row3['ALIASES']
                            strinstanceofid = row3['INSTANCE_OF']
                            strimagepath = row3['WIKIPEDIA_IMAGE_PATH']
                            cp.f_setservervariable("strsparqlcrawleritemfixinstanceofcurrentvalue",strwikidataid,"Current value in the current Wikidata SPARQL crawler",0)
                            cp.f_setservervariable("strsparqlcrawleritemfixinstanceofwikidataid",strwikidataid,"Current Wikidata ID in the current Wikidata SPARQL crawler",0)
                            strsqlperson = "SELECT * FROM T_WC_WIKIDATA_PERSON_V1 WHERE ID_WIKIDATA = '" + strwikidataid + "' "
                            cursor3.execute(strsqlperson)
                            lngrowcountperson = cursor3.rowcount
                            if lngrowcountperson == 0:
                                # Person does not exist in T_WC_WIKIDATA_PERSON_V1, we can move it
                                print(f"Moving {strwikidataid} {strname} to person")
                                arrpersoncouples = {}
                                arrpersoncouples["ID_WIKIDATA"] = strwikidataid
                                arrpersoncouples["NAME"] = strname
                                arrpersoncouples["ALIASES"] = straliases
                                arrpersoncouples["INSTANCE_OF"] = strinstanceofid
                                arrpersoncouples["WIKIPEDIA_PROFILE_PATH"] = strimagepath
                                strsqltablename = "T_WC_WIKIDATA_PERSON_V1"
                                strsqlupdatecondition = f"ID_WIKIDATA = '{strwikidataid}'"
                                cp.f_sqlupdatearray(strsqltablename,arrpersoncouples,strsqlupdatecondition,1)
                            else:
                                # Person already exists in T_WC_WIKIDATA_PERSON_V1, so we move only non empty values 
                                print(f"Updating {strwikidataid} {strname} in person")
                                results2 = cursor3.fetchall()
                                row2 = results2[0]
                                strnameperson = row2['NAME']
                                straliasesperson = row2['ALIASES']
                                strinstanceofperson = row2['INSTANCE_OF']
                                strwikipediaprofilepathperson = row2['WIKIPEDIA_PROFILE_PATH']
                                arrpersoncouples = {}
                                if strname != "" and (strnameperson == "" or strnameperson is None):
                                    arrpersoncouples["NAME"] = strname
                                if straliases != "" and (straliasesperson == "" or straliasesperson is None):
                                    arrpersoncouples["ALIASES"] = straliases
                                if strinstanceofid != "" and (strinstanceofperson == "" or strinstanceofperson is None):
                                    arrpersoncouples["INSTANCE_OF"] = strinstanceofid
                                if strimagepath != "" and (strwikipediaprofilepathperson == "" or strwikipediaprofilepathperson is None):
                                    arrpersoncouples["WIKIPEDIA_PROFILE_PATH"] = strimagepath
                                if arrpersoncouples:
                                    strsqltablename = "T_WC_WIKIDATA_PERSON_V1"
                                    strsqlupdatecondition = f"ID_WIKIDATA = '{strwikidataid}'"
                                    cp.f_sqlupdatearray(strsqltablename,arrpersoncouples,strsqlupdatecondition,1)
                            # After moving the item to person, we can delete it from T_WC_WIKIDATA_ITEM_V1
                            strsqldelete = "DELETE FROM T_WC_WIKIDATA_ITEM_V1 WHERE ID_WIKIDATA = '" + strwikidataid + "' "
                            print(f"{strsqldelete}")
                            cursor3.execute(strsqldelete)
                            cp.connectioncp.commit()
                if intindex == 111:
                    # T_WC_WIKIDATA_ITEM_PROPERTY de duplication
                    cp.f_setservervariable("strsparqlcrawleritemsdedupcurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL crawler",0)
                    strsql = ""
                    strsql += "SELECT * FROM T_WC_WIKIDATA_ITEM_PROPERTY ORDER BY ID_WIKIDATA, ID_PROPERTY, ID_ITEM "
                    #strsql += "LIMIT 500 "
                    if strsql != "":
                        print(strsql)
                        cursor.execute(strsql)
                        lngrowcount = cursor.rowcount
                        print(f"{lngrowcount} lines")
                        strwikidataidprev = ""
                        strpropertyidprev = ""
                        stritemidprev = ""
                        results = cursor.fetchall()
                        for row3 in results:
                            strwikidataid = row3['ID_WIKIDATA']
                            strpropertyid = row3['ID_PROPERTY']
                            stritemid = row3['ID_ITEM']
                            if strwikidataid == strwikidataidprev and strpropertyid == strpropertyidprev and stritemid == stritemidprev:
                                #This is a duplicate record
                                lngrowid = row3['ID_ROW']
                                strsqldelete = "DELETE FROM T_WC_WIKIDATA_ITEM_PROPERTY WHERE ID_ROW = " + str(lngrowid)
                                print(f"{strsqldelete}")
                                cursor3.execute(strsqldelete)
                                cp.connectioncp.commit()
                            strwikidataidprev = strwikidataid
                            strpropertyidprev = strpropertyid
                            stritemidprev = stritemid
                #cp.f_setservervariable("strsparqlcrawlercurrentsql","","Current SQL query in the SPARQL Wikidata crawler",0)
                cp.f_setservervariable("strsparqlcrawlercurrentvalue","","Current value in the current Wikidata SPARQL crawler",0)
                cp.f_setservervariable("strsparqlcrawleritemscurrentprocess","","Current process in the Wikidata SPARQL crawler",0)
            strcurrentprocess = ""
            cp.f_setservervariable("strsparqlcrawlercurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL crawler",0)
            strnow = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
            cp.f_setservervariable("strsparqlcrawlerenddatetime",strnow,"Date and time of the Wikidata SPARQL crawler ending",0)
            # Calculate total runtime and convert to readable format
            end_time = time.time()
            strtotalruntime = int(end_time - start_time)  # Total runtime in seconds
            cp.f_setservervariable("strsparqlcrawlertotalruntimesecond",str(strtotalruntime),strtotalruntimedesc,0)
            readable_duration = cp.convert_seconds_to_duration(strtotalruntime)
            cp.f_setservervariable("strsparqlcrawlertotalruntime",readable_duration,strtotalruntimedesc,0)
            print(f"Total runtime: {strtotalruntime} seconds ({readable_duration})")
            
    print("Process completed")
except pymysql.MySQLError as e:
    print(f"❌ MySQL Error: {e}")
    cp.connectioncp.rollback()

