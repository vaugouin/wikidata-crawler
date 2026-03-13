import requests
import json
import os
from dotenv import load_dotenv
import time
import pymysql.cursors
import citizenphil as cp
from datetime import datetime
import csv
import pandas as pd
import re
from SPARQLWrapper import SPARQLWrapper, SPARQLExceptions, JSON

# Load .env file 
load_dotenv()

strwikidatauseragent = os.getenv("WIKIMEDIA_USER_AGENT")
print("strwikidatauseragent",strwikidatauseragent)

def f_sparqlpersonscrawl(strwikidataidquery,lngyearquery=0):
    global strwikidatauseragent
    global strsparqlpersoninstanceof
    
    intencore = True
    while intencore:
        strsparqlquery = ""
        strsparqlquery += "SELECT ?item ?itemLabel ?imdbID ?tmdbID ?birthDate ?deathDate ?instanceOf "
        strsparqlquery += "WHERE { "
        if strwikidataidquery != "":
            strsparqlquery += "VALUES ?item { wd:" + strwikidataidquery + " } "
            strsparqlquery += "?item wdt:P31 ?instanceOf. "
            strsparqlquery += "OPTIONAL { ?item wdt:P345 ?imdbID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P4985 ?tmdbID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P569 ?birthDate. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P570 ?deathDate. } "
        else:
            arrpersoninstanceof = [s.strip() for s in strsparqlpersoninstanceof.split() if s.strip()]
            strpersoninstanceofwd = " ".join([f"wd:{s}" for s in arrpersoninstanceof])
            if strpersoninstanceofwd == "":
                strpersoninstanceofwd = "wd:Q5"
            strsparqlquery += "VALUES ?instanceOf { " + strpersoninstanceofwd + " } "
            strsparqlquery += "?item wdt:P31 ?instanceOf; "
            strsparqlquery += "wdt:P345 ?imdbID; "
            strsparqlquery += "wdt:P569 ?birthDate. "
            strsparqlquery += "?item wdt:P31 ?instanceOf. "
            strsparqlquery += "OPTIONAL { ?item wdt:P4985 ?tmdbID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P570 ?deathDate. } "
            if lngyearquery > 0:
                strsparqlquery += "FILTER(YEAR(?birthDate) = " + str(lngyearquery) + ") "
        strsparqlquery += "SERVICE wikibase:label { bd:serviceParam wikibase:language \"[AUTO_LANGUAGE],mul,en\". } "
        strsparqlquery += "} "
        strsparqlquery += "ORDER BY ?item "
        #strsparqlquery += "LIMIT " + str(lnglimit) + " "
        #strsparqlquery += "OFFSET " + str(lngoffset) + " "
        sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=strwikidatauseragent)
        # Set the query and return format
        print(strsparqlquery)
        sparql.setQuery(strsparqlquery)
        sparql.setReturnFormat(JSON)
        # Execute the query and convert the results
        try:
            query_result = sparql.query()
            results = query_result.convert()
            #print(results)
            # Convert the results to a Pandas DataFrame
            df = pd.json_normalize(results['results']['bindings'])
            lngcount = 0
            if not df.empty:
                #df = df[['item.value', 'itemLabel.value', 'imdbID.value', 'tmdbID.value', 'birthDate.value']]
                for index, row in df.iterrows():
                    lngcount += 1
                    print(row)
                    stritem = row['item.value']
                    # Compute strwikidataid
                    strwikidataid = ""
                    strwikidataid = stritem.split('/')[-1]
                    cp.f_setservervariable("strsparqlaltcrawlerpersonscurrentvalue",strwikidataid,"Current value in the current Wikidata SPARQL alternative crawler",0)
                    # Compute strname
                    strname = ""
                    if 'itemLabel.value' in row:
                        if row['itemLabel.value']:
                            if not pd.isna(row['itemLabel.value']):
                                strname = row['itemLabel.value']
                                # reject any name that looks like a Wikidata ID
                                if re.match(r'^[QPL]\d+$', strname):
                                    strname = ""
                    # Compute strimdbid
                    strimdbid = ""
                    if 'imdbID.value' in row:
                        if row['imdbID.value']:
                            if not pd.isna(row['imdbID.value']):
                                strimdbid = row['imdbID.value']
                                if len(strimdbid) > 10:
                                	strimdbid = strimdbid[:10]
                    # Compute lngtmdbid
                    lngtmdbid = 0
                    if 'tmdbID.value' in row:
                        if row['tmdbID.value']:
                            if not pd.isna(row['tmdbID.value']):
                                lngtmdbid = row['tmdbID.value']
                                #print(lngtmdbid)
                    # Compute birth date
                    strbirthdate = ""
                    strbirthdatesql = ""
                    if 'birthDate.value' in row:
                        if row['birthDate.value']:
                            if not pd.isna(row['birthDate.value']):
                                strbirthdate = row['birthDate.value']
                                #print(strbirthdate)
                                if strbirthdate != "":
                                    try:
                                        datbirthdate = datetime.strptime(strbirthdate, "%Y-%m-%dT%H:%M:%SZ")
                                        #strbirthdatesql = datbirthdate.strftime("%Y-%m-%d %H:%M:%S")
                                        strbirthdatesql = datbirthdate.strftime("%Y-%m-%d")
                                    except ValueError:
                                        # Handle the case where the string cannot be converted to a date
                                        #print(f"Invalid input: {strbirthdate} cannot be converted to a date.")
                                        strbirthdatesql = ""
                    # Compute death date
                    strdeathdate = ""
                    strdeathdatesql = ""
                    if 'deathDate.value' in row:
                        print("'deathDate.value' in row")
                        if row['deathDate.value']:
                            if not pd.isna(row['deathDate.value']):
                                strdeathdate = row['deathDate.value']
                                print(strdeathdate)
                                if strdeathdate != "":
                                    try:
                                        datdeathdate = datetime.strptime(strdeathdate, "%Y-%m-%dT%H:%M:%SZ")
                                        #strdeathdatesql = datdeathdate.strftime("%Y-%m-%d %H:%M:%S")
                                        strdeathdatesql = datdeathdate.strftime("%Y-%m-%d")
                                        print(strdeathdatesql)
                                    except ValueError:
                                        # Handle the case where the string cannot be converted to a date
                                        #print(f"Invalid input: {strdeathdate} cannot be converted to a date.")
                                        strdeathdatesql = ""
                    # Compute instance of
                    strinstanceof = ""
                    strinstanceofid = ""
                    if 'instanceOf.value' in row:
                        if row['instanceOf.value']:
                            if not pd.isna(row['instanceOf.value']):
                                strinstanceof = row['instanceOf.value']
                                strinstanceofid = strinstanceof.split('/')[-1]
                    print(f"{strwikidataid} '{strimdbid}' ID {lngtmdbid} '{strname}' {strbirthdatesql}-{strdeathdatesql}")
                    arrpersoncouples = {}
                    arrpersoncouples["ID_WIKIDATA"] = strwikidataid
                    arrpersoncouples["ID_PERSON"] = lngtmdbid
                    arrpersoncouples["ID_IMDB"] = strimdbid
                    arrpersoncouples["NAME"] = strname
                    if strbirthdatesql != "":
                        arrpersoncouples["BIRTHDAY"] = strbirthdatesql
                    if strdeathdatesql != "":
                        arrpersoncouples["DEATHDAY"] = strdeathdatesql
                        print("arrpersoncouples[\"DEATHDAY\"] = ",strdeathdatesql)
                    arrpersoncouples["INSTANCE_OF"] = strinstanceofid
                    strsqltablename = "T_WC_WIKIDATA_PERSON_V1"
                    strsqlupdatecondition = f"ID_WIKIDATA = '{strwikidataid}'"
                    cp.f_sqlupdatearray(strsqltablename,arrpersoncouples,strsqlupdatecondition,1)
            intencore = False
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

def f_sparqlmoviescrawl(strwikidataidquery,lngyearquery=0):
    global strwikidatauseragent
    global strsparqlmovieinstanceof
    
    strwikidataidprev = ""
    intencore = True
    while intencore:
        strsparqlquery = ""
        strsparqlquery += "SELECT ?item ?itemLabel ?imdbID ?tmdbID ?releaseDate ?genres ?plexMediaKey ?criterionFilmID ?criterionSpine ?color ?type "
        strsparqlquery += "WHERE { "
        if strwikidataidquery != "":
            strsparqlquery += "VALUES ?item { wd:" + strwikidataidquery + " } "
            strsparqlquery += "?item wdt:P31 ?type. "
            strsparqlquery += "OPTIONAL { ?item wdt:P345 ?imdbID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P4947 ?tmdbID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P577 ?releaseDate. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P136 ?genres. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P11460 ?plexMediaKey. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P9584 ?criterionFilmID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P12279 ?criterionSpine. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P462 ?color. } "
            strsparqlquery += "?item wdt:P577 ?pubdate. "
        else:
            arrmovieinstanceof = [s.strip() for s in strsparqlmovieinstanceof.split() if s.strip()]
            strmovieinstanceofwd = " ".join([f"wd:{s}" for s in arrmovieinstanceof])
            if strmovieinstanceofwd == "":
                strmovieinstanceofwd = "wd:Q11424"
            strsparqlquery += "VALUES ?type { " + strmovieinstanceofwd + " } "
            strsparqlquery += "?item wdt:P31 ?type. "
            strsparqlquery += "OPTIONAL { ?item wdt:P345 ?imdbID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P4947 ?tmdbID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P577 ?releaseDate. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P136 ?genres. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P11460 ?plexMediaKey. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P9584 ?criterionFilmID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P12279 ?criterionSpine. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P462 ?color. } "
            strsparqlquery += "?item wdt:P577 ?pubdate. "
            if lngyearquery > 0:
                strsparqlquery += "FILTER((?pubdate >= \"" + str(lngyearquery) + "-01-01T00:00:00Z\"^^xsd:dateTime) && (?pubdate <= \"" + str(lngyearquery) + "-12-31T00:00:00Z\"^^xsd:dateTime)) "
        strsparqlquery += "SERVICE wikibase:label { bd:serviceParam wikibase:language \"[AUTO_LANGUAGE],mul,en\". } "
        strsparqlquery += "} "
        strsparqlquery += "ORDER BY ?item DESC(?releaseDate) "
        #strsparqlquery += "LIMIT " + str(lnglimit) + " "
        #strsparqlquery += "OFFSET " + str(lngoffset) + " "
        sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=strwikidatauseragent)
        # Set the query and return format
        print(strsparqlquery)
        sparql.setQuery(strsparqlquery)
        sparql.setReturnFormat(JSON)
        # Execute the query and convert the results
        try:
            query_result = sparql.query()
            results = query_result.convert()
            #print(results)
            # Convert the results to a Pandas DataFrame
            df = pd.json_normalize(results['results']['bindings'])
            lngcount = 0
            if not df.empty:
                #df = df[['item.value', 'itemLabel.value', 'imdbID.value', 'tmdbID.value', 'birthDate.value']]
                for index, row in df.iterrows():
                    lngcount += 1
                    print(row)
                    stritem = row['item.value']
                    # Compute strwikidataid
                    strwikidataid = ""
                    strwikidataid = stritem.split('/')[-1]
                    cp.f_setservervariable("strsparqlaltcrawlermoviescurrentvalue",strwikidataid,"Current value in the current Wikidata SPARQL alternative crawler",0)
                    if strwikidataid != strwikidataidprev:
                        # We process a new movie
                        print("Processing a new movie")
                        #if strwikidataidprev != "":
                        if strwikidataidprev != "":
                            # Now delete genres that are not for the movie we just finished
                            if strgenrelist == "":
                                strgenrelist = "'0'"
                            strpropertyid = "P136"
                            strsqldelete = "DELETE FROM T_WC_WIKIDATA_ITEM_PROPERTY WHERE ID_WIKIDATA = '" + strwikidataidprev + "' AND ID_PROPERTY = '" + strpropertyid + "' AND ID_ITEM NOT IN (" + strgenrelist + ")"
                            print(f"{strsqldelete}")
                            cursor3.execute(strsqldelete)
                            cp.connectioncp.commit()
                            # Now delete colors that are not for the movie we just finished
                            if strcolorlist == "":
                                strcolorlist = "'0'"
                            strpropertyid = "P462"
                            strsqldelete = "DELETE FROM T_WC_WIKIDATA_ITEM_PROPERTY WHERE ID_WIKIDATA = '" + strwikidataidprev + "' AND ID_PROPERTY = '" + strpropertyid + "' AND ID_ITEM NOT IN (" + strcolorlist + ")"
                            print(f"{strsqldelete}")
                            cursor3.execute(strsqldelete)
                            cp.connectioncp.commit()
                        strgenrelist = ""
                        strcolorlist = ""
                        strwikidataidprev = strwikidataid
                    # Compute strtitle
                    strtitle = ""
                    if 'itemLabel.value' in row:
                        if row['itemLabel.value']:
                            if not pd.isna(row['itemLabel.value']):
                                strtitle = row['itemLabel.value']
                                # reject any title that looks like a Wikidata ID
                                if re.match(r'^[QPL]\d+$', strtitle):
                                    strtitle = ""
                    # Compute strimdbid
                    strimdbid = ""
                    if 'imdbID.value' in row:
                        if row['imdbID.value']:
                            if not pd.isna(row['imdbID.value']):
                                strimdbid = row['imdbID.value']
                                if len(strimdbid) > 10:
                                	strimdbid = strimdbid[:10]
                    # Compute lngtmdbid
                    lngtmdbid = 0
                    if 'tmdbID.value' in row:
                        if row['tmdbID.value']:
                            if not pd.isna(row['tmdbID.value']):
                                lngtmdbid = row['tmdbID.value']
                                #print(lngtmdbid)
                    # Compute release date
                    strreleasedate = ""
                    strreleasedatesql = ""
                    if 'releaseDate.value' in row:
                        if row['releaseDate.value']:
                            if not pd.isna(row['releaseDate.value']):
                                strreleasedate = row['releaseDate.value']
                                #print(strreleasedate)
                                if strreleasedate != "":
                                    try:
                                        datreleasedate = datetime.strptime(strreleasedate, "%Y-%m-%dT%H:%M:%SZ")
                                        #strreleasedatesql = datreleasedate.strftime("%Y-%m-%d %H:%M:%S")
                                        strreleasedatesql = datreleasedate.strftime("%Y-%m-%d")
                                    except ValueError:
                                        # Handle the case where the string cannot be converted to a date
                                        #print(f"Invalid input: {strreleasedate} cannot be converted to a date.")
                                        strreleasedatesql = ""
                    # Compute strplexmediakey
                    strplexmediakey = ""
                    if 'plexMediaKey.value' in row:
                        if row['plexMediaKey.value']:
                            if not pd.isna(row['plexMediaKey.value']):
                                strplexmediakey = row['plexMediaKey.value']
                    # Compute lngcriterionfilmid
                    lngcriterionfilmid = 0
                    if 'criterionFilmID.value' in row:
                        if row['criterionFilmID.value']:
                            if not pd.isna(row['criterionFilmID.value']):
                                strcriterionfilmid = row['criterionFilmID.value']
                                #print(strcriterionfilmid)
                                try:
                                    #print(strcriterionfilmid)
                                    lngcriterionfilmid = int(strcriterionfilmid)
                                    #print(f"Converted integer: {lngcriterionfilmid}")
                                except ValueError:
                                    # Handle the case where the string cannot be converted to an integer
                                    #print(f"Invalid input: {strcriterionfilmid} cannot be converted to an integer.")
                                    lngcriterionfilmid = 0
                    # Compute lngcriterionspine
                    lngcriterionspine = 0
                    if 'criterionSpine.value' in row:
                        if row['criterionSpine.value']:
                            if not pd.isna(row['criterionSpine.value']):
                                strcriterionspine = row['criterionSpine.value']
                                #print(strcriterionspine)
                                try:
                                    #print(strcriterionspine)
                                    lngcriterionspine = int(strcriterionspine)
                                    #print(f"Converted integer: {lngcriterionspine}")
                                except ValueError:
                                    # Handle the case where the string cannot be converted to an integer
                                    #print(f"Invalid input: {strcriterionspine} cannot be converted to an integer.")
                                    lngcriterionspine = 0
                    # Compute strinstanceof
                    strinstanceof = ""
                    strinstanceofid = ""
                    if 'type.value' in row:
                        if row['type.value']:
                            if not pd.isna(row['type.value']):
                                strinstanceof = row['type.value']
                                strinstanceofid = strinstanceof.split('/')[-1]
                    # Compute strgenre
                    strgenre = ""
                    strgenreid = ""
                    if 'genres.value' in row:
                        if row['genres.value']:
                            if not pd.isna(row['genres.value']):
                                strgenre = row['genres.value']
                                strgenreid = strgenre.split('/')[-1]
                                if strgenre != "":
                                    if strgenrelist != "":
                                        strgenrelist += ","
                                    strgenrelist += "'" + strgenreid + "'"
                                    strpropertyid = "P136"
                                    arrmoviecouples = {}
                                    arrmoviecouples["ID_WIKIDATA"] = strwikidataid
                                    arrmoviecouples["ID_PROPERTY"] = strpropertyid
                                    arrmoviecouples["ID_ITEM"] = strgenreid
                                    strsqltablename = "T_WC_WIKIDATA_ITEM_PROPERTY"
                                    strsqlupdatecondition = f"ID_WIKIDATA = '{strwikidataid}' AND ID_PROPERTY = '{strpropertyid}' AND ID_ITEM = '{strgenreid}'"
                                    cp.f_sqlupdatearray(strsqltablename,arrmoviecouples,strsqlupdatecondition,1)
                    # Compute strcolor
                    strcolor = ""
                    strcolorid = ""
                    if 'color.value' in row:
                        if row['color.value']:
                            if not pd.isna(row['color.value']):
                                strcolor = row['color.value']
                                strcolorid = strcolor.split('/')[-1]
                                if strcolor != "":
                                    if strcolorlist != "":
                                        strcolorlist += ","
                                    strcolorlist += "'" + strcolorid + "'"
                                    strpropertyid = "P462"
                                    arrmoviecouples = {}
                                    arrmoviecouples["ID_WIKIDATA"] = strwikidataid
                                    arrmoviecouples["ID_PROPERTY"] = strpropertyid
                                    arrmoviecouples["ID_ITEM"] = strcolorid
                                    strsqltablename = "T_WC_WIKIDATA_ITEM_PROPERTY"
                                    strsqlupdatecondition = f"ID_WIKIDATA = '{strwikidataid}' AND ID_PROPERTY = '{strpropertyid}' AND ID_ITEM = '{strcolorid}'"
                                    cp.f_sqlupdatearray(strsqltablename,arrmoviecouples,strsqlupdatecondition,1)
                    strmessage = f"{strwikidataid} '{strimdbid}' ID {lngtmdbid} '{strtitle}' {strreleasedatesql} Plex: {strplexmediakey} genre: {strgenreid}"
                    print(strmessage)
                    arrmoviecouples = {}
                    arrmoviecouples["ID_WIKIDATA"] = strwikidataid
                    arrmoviecouples["ID_MOVIE"] = lngtmdbid
                    arrmoviecouples["ID_IMDB"] = strimdbid
                    arrmoviecouples["TITLE"] = strtitle
                    if strplexmediakey != "":
                        arrmoviecouples["PLEX_MEDIA_KEY"] = strplexmediakey
                    if strreleasedatesql != "":
                        arrmoviecouples["DAT_RELEASE"] = strreleasedatesql
                    arrmoviecouples["ID_CRITERION"] = lngcriterionfilmid
                    arrmoviecouples["ID_CRITERION_SPINE"] = lngcriterionspine
                    arrmoviecouples["INSTANCE_OF"] = strinstanceofid
                    
                    strsqltablename = "T_WC_WIKIDATA_MOVIE_V1"
                    strsqlupdatecondition = f"ID_WIKIDATA = '{strwikidataid}'"
                    cp.f_sqlupdatearray(strsqltablename,arrmoviecouples,strsqlupdatecondition,1)
                # End of the loop for the current query so we process the last movie
                if strwikidataidprev != "":
                    # We process a new movie
                    print("Processing the last movie of the query")
                    # Now delete genres that are not for the movie we just finished
                    if strgenrelist == "":
                        strgenrelist = "'0'"
                    strpropertyid = "P136"
                    strsqldelete = "DELETE FROM T_WC_WIKIDATA_ITEM_PROPERTY WHERE ID_WIKIDATA = '" + strwikidataidprev + "' AND ID_PROPERTY = '" + strpropertyid + "' AND ID_ITEM NOT IN (" + strgenrelist + ")"
                    print(f"{strsqldelete}")
                    cursor3.execute(strsqldelete)
                    cp.connectioncp.commit()
                    strgenrelist = ""
                    # Now delete colors that are not for the movie we just finished
                    if strcolorlist == "":
                        strcolorlist = "'0'"
                    strpropertyid = "P462"
                    strsqldelete = "DELETE FROM T_WC_WIKIDATA_ITEM_PROPERTY WHERE ID_WIKIDATA = '" + strwikidataidprev + "' AND ID_PROPERTY = '" + strpropertyid + "' AND ID_ITEM NOT IN (" + strcolorlist + ")"
                    print(f"{strsqldelete}")
                    cursor3.execute(strsqldelete)
                    cp.connectioncp.commit()
                    strcolorlist = ""
                    
                    strwikidataidprev = ""
            intencore = False
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

def f_sparqlseriescrawl(strwikidataidquery,lngyearquery=0):
    global strwikidatauseragent
    global strsparqlserieinstanceof
    
    strwikidataidprev = ""
    intencore = True
    while intencore:
        strsparqlquery = ""
        strsparqlquery += "SELECT ?item ?itemLabel ?imdbID ?tmdbID ?startTime ?endTime ?genres ?plexMediaKey ?criterionFilmID ?criterionSpine ?color ?type "
        strsparqlquery += "WHERE { "
        if strwikidataidquery != "":
            strsparqlquery += "VALUES ?item { wd:" + strwikidataidquery + " } "
            strsparqlquery += "?item wdt:P31 ?type. "
            strsparqlquery += "OPTIONAL { ?item wdt:P345 ?imdbID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P4947 ?tmdbID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P580 ?startTime. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P582 ?endTime. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P136 ?genres. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P11460 ?plexMediaKey. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P9584 ?criterionFilmID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P12279 ?criterionSpine. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P462 ?color. } "
            strsparqlquery += "?item wdt:P580 ?pubdate. "
        else:
            arrserieinstanceof = [s.strip() for s in strsparqlserieinstanceof.split() if s.strip()]
            strserieinstanceofwd = " ".join([f"wd:{s}" for s in arrserieinstanceof])
            if strserieinstanceofwd == "":
                strserieinstanceofwd = "wd:Q5398426"
            strsparqlquery += "VALUES ?type { " + strserieinstanceofwd + " } "
            strsparqlquery += "?item wdt:P31 ?type. "
            strsparqlquery += "OPTIONAL { ?item wdt:P345 ?imdbID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P4947 ?tmdbID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P580 ?startTime. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P582 ?endTime. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P136 ?genres. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P11460 ?plexMediaKey. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P9584 ?criterionFilmID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P12279 ?criterionSpine. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P462 ?color. } "
            strsparqlquery += "?item wdt:P580 ?pubdate. "
            if lngyearquery > 0:
                strsparqlquery += "FILTER((?pubdate >= \"" + str(lngyearquery) + "-01-01T00:00:00Z\"^^xsd:dateTime) && (?pubdate <= \"" + str(lngyearquery) + "-12-31T00:00:00Z\"^^xsd:dateTime)) "
        strsparqlquery += "SERVICE wikibase:label { bd:serviceParam wikibase:language \"[AUTO_LANGUAGE],mul,en\". } "
        strsparqlquery += "} "
        strsparqlquery += "ORDER BY ?item DESC(?startTime) "
        #strsparqlquery += "LIMIT " + str(lnglimit) + " "
        #strsparqlquery += "OFFSET " + str(lngoffset) + " "
        sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=strwikidatauseragent)
        # Set the query and return format
        print(strsparqlquery)
        sparql.setQuery(strsparqlquery)
        sparql.setReturnFormat(JSON)
        # Execute the query and convert the results
        try:
            query_result = sparql.query()
            results = query_result.convert()
            #print(results)
            # Convert the results to a Pandas DataFrame
            df = pd.json_normalize(results['results']['bindings'])
            lngcount = 0
            if not df.empty:
                #df = df[['item.value', 'itemLabel.value', 'imdbID.value', 'tmdbID.value', 'birthDate.value']]
                for index, row in df.iterrows():
                    lngcount += 1
                    print(row)
                    stritem = row['item.value']
                    # Compute strwikidataid
                    strwikidataid = ""
                    strwikidataid = stritem.split('/')[-1]
                    cp.f_setservervariable("strsparqlaltcrawlerseriescurrentvalue",strwikidataid,"Current value in the current Wikidata SPARQL alternative crawler",0)
                    if strwikidataid != strwikidataidprev:
                        # We process a new serie
                        print("Processing a new serie")
                        #if strwikidataidprev != "":
                        if strwikidataidprev != "":
                            # Now delete genres that are not for the serie we just finished
                            if strgenrelist == "":
                                strgenrelist = "'0'"
                            strpropertyid = "P136"
                            strsqldelete = "DELETE FROM T_WC_WIKIDATA_ITEM_PROPERTY WHERE ID_WIKIDATA = '" + strwikidataidprev + "' AND ID_PROPERTY = '" + strpropertyid + "' AND ID_ITEM NOT IN (" + strgenrelist + ")"
                            print(f"{strsqldelete}")
                            cursor3.execute(strsqldelete)
                            cp.connectioncp.commit()
                            # Now delete colors that are not for the serie we just finished
                            if strcolorlist == "":
                                strcolorlist = "'0'"
                            strpropertyid = "P462"
                            strsqldelete = "DELETE FROM T_WC_WIKIDATA_ITEM_PROPERTY WHERE ID_WIKIDATA = '" + strwikidataidprev + "' AND ID_PROPERTY = '" + strpropertyid + "' AND ID_ITEM NOT IN (" + strcolorlist + ")"
                            print(f"{strsqldelete}")
                            cursor3.execute(strsqldelete)
                            cp.connectioncp.commit()
                        strgenrelist = ""
                        strcolorlist = ""
                        strwikidataidprev = strwikidataid
                    # Compute strtitle
                    strtitle = ""
                    if 'itemLabel.value' in row:
                        if row['itemLabel.value']:
                            if not pd.isna(row['itemLabel.value']):
                                strtitle = row['itemLabel.value']
                                # reject any title that looks like a Wikidata ID
                                if re.match(r'^[QPL]\d+$', strtitle):
                                    strtitle = ""
                    # Compute strimdbid
                    strimdbid = ""
                    if 'imdbID.value' in row:
                        if row['imdbID.value']:
                            if not pd.isna(row['imdbID.value']):
                                strimdbid = row['imdbID.value']
                                if len(strimdbid) > 10:
                                	strimdbid = strimdbid[:10]
                    # Compute lngtmdbid
                    lngtmdbid = 0
                    if 'tmdbID.value' in row:
                        if row['tmdbID.value']:
                            if not pd.isna(row['tmdbID.value']):
                                lngtmdbid = row['tmdbID.value']
                                #print(lngtmdbid)
                    # Compute start date
                    strstartdate = ""
                    strstartdatesql = ""
                    if 'startTime.value' in row:
                        if row['startTime.value']:
                            if not pd.isna(row['startTime.value']):
                                strstartdate = row['startTime.value']
                                #print(strstartdate)
                                if strstartdate != "":
                                    try:
                                        datreleasedate = datetime.strptime(strstartdate, "%Y-%m-%dT%H:%M:%SZ")
                                        #strstartdatesql = datreleasedate.strftime("%Y-%m-%d %H:%M:%S")
                                        strstartdatesql = datreleasedate.strftime("%Y-%m-%d")
                                    except ValueError:
                                        # Handle the case where the string cannot be converted to a date
                                        #print(f"Invalid input: {strstartdate} cannot be converted to a date.")
                                        strstartdatesql = ""
                    # Compute end date
                    strenddate = ""
                    strenddatesql = ""
                    if 'endTime.value' in row:
                        if row['endTime.value']:
                            if not pd.isna(row['endTime.value']):
                                strenddate = row['endTime.value']
                                #print(strenddate)
                                if strenddate != "":
                                    try:
                                        datreleasedate = datetime.strptime(strenddate, "%Y-%m-%dT%H:%M:%SZ")
                                        #strenddatesql = datreleasedate.strftime("%Y-%m-%d %H:%M:%S")
                                        strenddatesql = datreleasedate.strftime("%Y-%m-%d")
                                    except ValueError:
                                        # Handle the case where the string cannot be converted to a date
                                        #print(f"Invalid input: {strenddate} cannot be converted to a date.")
                                        strenddatesql = ""
                    # Compute strplexmediakey
                    strplexmediakey = ""
                    if 'plexMediaKey.value' in row:
                        if row['plexMediaKey.value']:
                            if not pd.isna(row['plexMediaKey.value']):
                                strplexmediakey = row['plexMediaKey.value']
                    # Compute lngcriterionfilmid
                    lngcriterionfilmid = 0
                    if 'criterionFilmID.value' in row:
                        if row['criterionFilmID.value']:
                            if not pd.isna(row['criterionFilmID.value']):
                                strcriterionfilmid = row['criterionFilmID.value']
                                #print(strcriterionfilmid)
                                try:
                                    #print(strcriterionfilmid)
                                    lngcriterionfilmid = int(strcriterionfilmid)
                                    #print(f"Converted integer: {lngcriterionfilmid}")
                                except ValueError:
                                    # Handle the case where the string cannot be converted to an integer
                                    #print(f"Invalid input: {strcriterionfilmid} cannot be converted to an integer.")
                                    lngcriterionfilmid = 0
                    # Compute lngcriterionspine
                    lngcriterionspine = 0
                    if 'criterionSpine.value' in row:
                        if row['criterionSpine.value']:
                            if not pd.isna(row['criterionSpine.value']):
                                strcriterionspine = row['criterionSpine.value']
                                #print(strcriterionspine)
                                try:
                                    #print(strcriterionspine)
                                    lngcriterionspine = int(strcriterionspine)
                                    #print(f"Converted integer: {lngcriterionspine}")
                                except ValueError:
                                    # Handle the case where the string cannot be converted to an integer
                                    #print(f"Invalid input: {strcriterionspine} cannot be converted to an integer.")
                                    lngcriterionspine = 0
                    # Compute strinstanceof
                    strinstanceof = ""
                    strinstanceofid = ""
                    if 'type.value' in row:
                        if row['type.value']:
                            if not pd.isna(row['type.value']):
                                strinstanceof = row['type.value']
                                strinstanceofid = strinstanceof.split('/')[-1]
                    # Compute strgenre
                    strgenre = ""
                    strgenreid = ""
                    if 'genres.value' in row:
                        if row['genres.value']:
                            if not pd.isna(row['genres.value']):
                                strgenre = row['genres.value']
                                strgenreid = strgenre.split('/')[-1]
                                if strgenre != "":
                                    if strgenrelist != "":
                                        strgenrelist += ","
                                    strgenrelist += "'" + strgenreid + "'"
                                    strpropertyid = "P136"
                                    arrseriecouples = {}
                                    arrseriecouples["ID_WIKIDATA"] = strwikidataid
                                    arrseriecouples["ID_PROPERTY"] = strpropertyid
                                    arrseriecouples["ID_ITEM"] = strgenreid
                                    strsqltablename = "T_WC_WIKIDATA_ITEM_PROPERTY"
                                    strsqlupdatecondition = f"ID_WIKIDATA = '{strwikidataid}' AND ID_PROPERTY = '{strpropertyid}' AND ID_ITEM = '{strgenreid}'"
                                    cp.f_sqlupdatearray(strsqltablename,arrseriecouples,strsqlupdatecondition,1)
                    # Compute strcolor
                    strcolor = ""
                    strcolorid = ""
                    if 'color.value' in row:
                        if row['color.value']:
                            if not pd.isna(row['color.value']):
                                strcolor = row['color.value']
                                strcolorid = strcolor.split('/')[-1]
                                if strcolor != "":
                                    if strcolorlist != "":
                                        strcolorlist += ","
                                    strcolorlist += "'" + strcolorid + "'"
                                    strpropertyid = "P462"
                                    arrseriecouples = {}
                                    arrseriecouples["ID_WIKIDATA"] = strwikidataid
                                    arrseriecouples["ID_PROPERTY"] = strpropertyid
                                    arrseriecouples["ID_ITEM"] = strcolorid
                                    strsqltablename = "T_WC_WIKIDATA_ITEM_PROPERTY"
                                    strsqlupdatecondition = f"ID_WIKIDATA = '{strwikidataid}' AND ID_PROPERTY = '{strpropertyid}' AND ID_ITEM = '{strcolorid}'"
                                    cp.f_sqlupdatearray(strsqltablename,arrseriecouples,strsqlupdatecondition,1)
                    strmessage = f"{strwikidataid} '{strimdbid}' ID {lngtmdbid} '{strtitle}' {strstartdatesql}-{strenddatesql} Plex: {strplexmediakey} genre: {strgenreid}"
                    print(strmessage)
                    arrseriecouples = {}
                    arrseriecouples["ID_WIKIDATA"] = strwikidataid
                    arrseriecouples["ID_SERIE"] = lngtmdbid
                    arrseriecouples["ID_IMDB"] = strimdbid
                    arrseriecouples["TITLE"] = strtitle
                    if strplexmediakey != "":
                        arrseriecouples["PLEX_MEDIA_KEY"] = strplexmediakey
                    if strstartdatesql != "":
                        arrseriecouples["DAT_START"] = strstartdatesql
                    if strenddatesql != "":
                        arrseriecouples["DAT_END"] = strenddatesql
                    arrseriecouples["ID_CRITERION"] = lngcriterionfilmid
                    arrseriecouples["ID_CRITERION_SPINE"] = lngcriterionspine
                    arrseriecouples["INSTANCE_OF"] = strinstanceofid
                    
                    strsqltablename = "T_WC_WIKIDATA_SERIE_V1"
                    strsqlupdatecondition = f"ID_WIKIDATA = '{strwikidataid}'"
                    cp.f_sqlupdatearray(strsqltablename,arrseriecouples,strsqlupdatecondition,1)
                # End of the loop for the current query so we process the last serie
                if strwikidataidprev != "":
                    # We process a new serie
                    print("Processing the last serie of the query")
                    # Now delete genres that are not for the serie we just finished
                    if strgenrelist == "":
                        strgenrelist = "'0'"
                    strpropertyid = "P136"
                    strsqldelete = "DELETE FROM T_WC_WIKIDATA_ITEM_PROPERTY WHERE ID_WIKIDATA = '" + strwikidataidprev + "' AND ID_PROPERTY = '" + strpropertyid + "' AND ID_ITEM NOT IN (" + strgenrelist + ")"
                    print(f"{strsqldelete}")
                    cursor3.execute(strsqldelete)
                    cp.connectioncp.commit()
                    strgenrelist = ""
                    # Now delete colors that are not for the serie we just finished
                    if strcolorlist == "":
                        strcolorlist = "'0'"
                    strpropertyid = "P462"
                    strsqldelete = "DELETE FROM T_WC_WIKIDATA_ITEM_PROPERTY WHERE ID_WIKIDATA = '" + strwikidataidprev + "' AND ID_PROPERTY = '" + strpropertyid + "' AND ID_ITEM NOT IN (" + strcolorlist + ")"
                    print(f"{strsqldelete}")
                    cursor3.execute(strsqldelete)
                    cp.connectioncp.commit()
                    strcolorlist = ""
                    
                    strwikidataidprev = ""
            intencore = False
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

strprocessesexecutedprevious = cp.f_getservervariable("strsparqlaltcrawlermoviespersonsprocessesexecuted",0)
strprocessesexecuteddesc = "List of processes executed in the Wikidata SPARQL alternative crawler"
cp.f_setservervariable("strsparqlaltcrawlerprocessesexecutedprevious",strprocessesexecutedprevious,strprocessesexecuteddesc + " (previous execution)",0)
strprocessesexecuted = ""
cp.f_setservervariable("strsparqlaltcrawlerprocessesexecuted",strprocessesexecuted,strprocessesexecuteddesc,0)

try:
    with cp.connectioncp:
        with cp.connectioncp.cursor() as cursor:
            cursor3 = cp.connectioncp.cursor()
            # Start timing the script execution
            start_time = time.time()
            strnow = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
            cp.f_setservervariable("strsparqlaltcrawlerstartdatetime",strnow,"Date and time of the last start of the Wikidata SPARQL alternative crawler",0)
            strtotalruntimedesc = "Total runtime of the Wikidata SPARQL crawler for movies, series and persons"
            strtotalruntimeprevious = cp.f_getservervariable("strsparqlaltcrawlermoviespersonstotalruntime",0)
            cp.f_setservervariable("strsparqlaltcrawlertotalruntimeprevious",strtotalruntimeprevious,strtotalruntimedesc + " (previous execution)",0)
            strtotalruntime = ""
            cp.f_setservervariable("strsparqlaltcrawlertotalruntime",strtotalruntime,strtotalruntimedesc,0)
            # Request Homer
            #f_sparqlpersonscrawl("Q6691",0)
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
            #arrwikidatascope = {101: 'movie', 102: 'person'}
            arrwikidatascope = {103: 'item to person', 104: 'item to movie', 106: 'item to serie', 102: 'person', 101: 'movie', 105: 'serie'}
            #arrwikidatascope = {104: 'item to movie'}
            #arrwikidatascope = {103: 'item to person'}
            #arrwikidatascope = {105: 'serie'}
            #arrwikidatascope = {106: 'item to serie'}
            for intindex,strcontent in arrwikidatascope.items():
                strcurrentprocess = f"{intindex}: processing Wikidata " + strcontent + " data using SPARQL"
                strprocessesexecuted += str(intindex) + ", "
                cp.f_setservervariable("strsparqlaltcrawlerprocessesexecuted",strprocessesexecuted,strprocessesexecuteddesc,0)
                print(strcurrentprocess)
                if intindex == 101:
                    # Films data download
                    lngoffset = -1
                    lngyearbegin = datetime.now().year + 4
                    #lngyearbegin = 2025
                    lngyearend = 1875
                    #lngyearend = 2025
                    lngyearquery = lngyearbegin
                    intencore = True
                    while intencore:
                        cp.f_setservervariable("strsparqlaltcrawlermoviescurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL alternative crawler",0)
                        cp.f_setservervariable("strsparqlaltcrawlermoviescurrentvalue",str(lngyearquery),"Current year in the Wikidata SPARQL alternative crawler, movie process",0)
                        time.sleep(90)
                        # Retrieve all movies for a given year
                        print("lngyearquery = " + str(lngyearquery))
                        f_sparqlmoviescrawl("",lngyearquery)
                        if lngyearquery < lngyearend:
                            intencore = False
                        else:
                            lngyearquery += lngoffset
                elif intindex == 102:
                    # Humans data download
                    lngoffset = -1
                    lngyearbegin = datetime.now().year
                    lngyearend = 1000
                    lngyearquery = lngyearbegin
                    intencore = True
                    while intencore:
                        cp.f_setservervariable("strsparqlaltcrawlerpersonscurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL alternative crawler",0)
                        cp.f_setservervariable("strsparqlaltcrawlerpersonscurrentvalue",str(lngyearquery),"Current year in the Wikidata SPARQL alternative crawler, person process",0)
                        time.sleep(5)
                        # Retrieve all persons for a given year
                        print("lngyearquery = " + str(lngyearquery))
                        f_sparqlpersonscrawl("",lngyearquery)
                        if lngyearquery < lngyearend:
                            intencore = False
                        else:
                            lngyearquery += lngoffset
                elif intindex == 103:
                    # Items to persons data download
                    strsql = ""
                    strsql += "SELECT DISTINCT ID_WIKIDATA "
                    strsql += "FROM T_WC_WIKIDATA_ITEM_V1 "
                    arrpersoninstanceof = [s.strip() for s in strsparqlpersoninstanceof.split() if s.strip()]
                    strpersoninstanceofsql = ", ".join([f"'{s}'" for s in arrpersoninstanceof])
                    if strpersoninstanceofsql == "":
                        strpersoninstanceofsql = "'0'"
                    strsql += "WHERE INSTANCE_OF IN (" + strpersoninstanceofsql + ") "
                    #strsql += "AND ID_WIKIDATA NOT IN ( "
                    #strsql += "SELECT ID_WIKIDATA FROM T_WC_WIKIDATA_PERSON_V1 "
                    #strsql += ") "
                    strsql += "ORDER BY ID_WIKIDATA "
                    # strsql += "LIMIT 1 "
                    if strsql != "":
                        print(strsql)
                        cursor3.execute(strsql)
                        lngrowcount = cursor3.rowcount
                        print(f"{lngrowcount} lines")
                        results = cursor3.fetchall()
                        for row3 in results:
                            strwikidataid = row3['ID_WIKIDATA']
                            cp.f_setservervariable("strsparqlaltcrawleritemstopersonscurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL alternative crawler",0)
                            cp.f_setservervariable("strsparqlaltcrawleritemstopersonscurrentvalue",strwikidataid,"Current Wikidata id in the Wikidata SPARQL alternative crawler, person process",0)
                            time.sleep(2)
                            # Retrieve the person for the given wikidata id 
                            print("strwikidataid = " + strwikidataid)
                            f_sparqlpersonscrawl(strwikidataid,0)
                            # Now delete this ID_WIKIDATA in T_WC_WIKIDATA_ITEM_V1
                            strsqldelete = ""
                            strsqldelete += "DELETE FROM T_WC_WIKIDATA_ITEM_V1 "
                            strsqldelete += "WHERE ID_WIKIDATA = '" + strwikidataid + "' "
                            cursor3.execute(strsqldelete)
                elif intindex == 104:
                    # Items to movies data download
                    strsql = ""
                    strsql += "SELECT DISTINCT ID_WIKIDATA "
                    strsql += "FROM T_WC_WIKIDATA_ITEM_V1 "
                    arrmovieinstanceof = [s.strip() for s in strsparqlmovieinstanceof.split() if s.strip()]
                    strmovieinstanceofsql = ", ".join([f"'{s}'" for s in arrmovieinstanceof])
                    if strmovieinstanceofsql == "":
                        strmovieinstanceofsql = "'0'"
                    strsql += "WHERE INSTANCE_OF IN (" + strmovieinstanceofsql + ") "
                    #strsql += "AND ID_WIKIDATA NOT IN ( "
                    #strsql += "SELECT ID_WIKIDATA FROM T_WC_WIKIDATA_MOVIE_V1 "
                    #strsql += ") "
                    strsql += "ORDER BY ID_WIKIDATA "
                    # strsql += "LIMIT 1 "
                    if strsql != "":
                        print(strsql)
                        cursor3.execute(strsql)
                        lngrowcount = cursor3.rowcount
                        print(f"{lngrowcount} lines")
                        results = cursor3.fetchall()
                        for row3 in results:
                            strwikidataid = row3['ID_WIKIDATA']
                            cp.f_setservervariable("strsparqlaltcrawleritemstomoviescurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL alternative crawler",0)
                            cp.f_setservervariable("strsparqlaltcrawleritemstomoviescurrentvalue",strwikidataid,"Current Wikidata id in the Wikidata SPARQL alternative crawler, movie process",0)
                            time.sleep(2)
                            # Retrieve the person for the given wikidata id 
                            print("strwikidataid = " + strwikidataid)
                            f_sparqlmoviescrawl(strwikidataid,0)
                            # Now delete this ID_WIKIDATA in T_WC_WIKIDATA_ITEM_V1
                            strsqldelete = ""
                            strsqldelete += "DELETE FROM T_WC_WIKIDATA_ITEM_V1 "
                            strsqldelete += "WHERE ID_WIKIDATA = '" + strwikidataid + "' "
                            cursor3.execute(strsqldelete)
                elif intindex == 106:
                    # Items to series data download
                    strsql = ""
                    strsql += "SELECT DISTINCT ID_WIKIDATA "
                    strsql += "FROM T_WC_WIKIDATA_ITEM_V1 "
                    arrserieinstanceof = [s.strip() for s in strsparqlserieinstanceof.split() if s.strip()]
                    strserieinstanceofsql = ", ".join([f"'{s}'" for s in arrserieinstanceof])
                    if strserieinstanceofsql == "":
                        strserieinstanceofsql = "'0'"
                    strsql += "WHERE INSTANCE_OF IN (" + strserieinstanceofsql + ") "
                    #strsql += "AND ID_WIKIDATA NOT IN ( "
                    #strsql += "SELECT ID_WIKIDATA FROM T_WC_WIKIDATA_SERIE_V1 "
                    #strsql += ") "
                    strsql += "ORDER BY ID_WIKIDATA "
                    # strsql += "LIMIT 1 "
                    if strsql != "":
                        print(strsql)
                        cursor3.execute(strsql)
                        lngrowcount = cursor3.rowcount
                        print(f"{lngrowcount} lines")
                        results = cursor3.fetchall()
                        for row3 in results:
                            strwikidataid = row3['ID_WIKIDATA']
                            cp.f_setservervariable("strsparqlaltcrawleritemstoseriescurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL alternative crawler",0)
                            cp.f_setservervariable("strsparqlaltcrawleritemstoseriescurrentvalue",strwikidataid,"Current Wikidata id in the Wikidata SPARQL alternative crawler, series process",0)
                            time.sleep(2)
                            # Retrieve the person for the given wikidata id 
                            print("strwikidataid = " + strwikidataid)
                            f_sparqlseriescrawl(strwikidataid,0)
                            # Now delete this ID_WIKIDATA in T_WC_WIKIDATA_ITEM_V1
                            strsqldelete = ""
                            strsqldelete += "DELETE FROM T_WC_WIKIDATA_ITEM_V1 "
                            strsqldelete += "WHERE ID_WIKIDATA = '" + strwikidataid + "' "
                            cursor3.execute(strsqldelete)
                elif intindex == 105:
                    # Series data download
                    lngoffset = -1
                    lngyearbegin = datetime.now().year + 4
                    #lngyearbegin = 1999
                    lngyearend = 1925
                    #lngyearend = 2025
                    lngyearquery = lngyearbegin
                    intencore = True
                    while intencore:
                        cp.f_setservervariable("strsparqlaltcrawlerseriescurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL alternative crawler",0)
                        cp.f_setservervariable("strsparqlaltcrawlerseriescurrentvalue",str(lngyearquery),"Current year in the Wikidata SPARQL alternative crawler, serie process",0)
                        time.sleep(90)
                        # Retrieve all series for a given year
                        print("lngyearquery = " + str(lngyearquery))
                        f_sparqlseriescrawl("",lngyearquery)
                        if lngyearquery < lngyearend:
                            intencore = False
                        else:
                            lngyearquery += lngoffset
            strcurrentprocess = ""
            cp.f_setservervariable("strsparqlaltcrawlercurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL crawler",0)
            strnow = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
            cp.f_setservervariable("strsparqlaltcrawlerenddatetime",strnow,"Date and time of the last end of the Wikidata SPARQL alternative crawler",0)
            # Calculate total runtime and convert to readable format
            end_time = time.time()
            strtotalruntime = int(end_time - start_time)  # Total runtime in seconds
            cp.f_setservervariable("strsparqlaltcrawlertotalruntimesecond",str(strtotalruntime),strtotalruntimedesc,0)
            readable_duration = cp.convert_seconds_to_duration(strtotalruntime)
            cp.f_setservervariable("strsparqlaltcrawlertotalruntime",readable_duration,strtotalruntimedesc,0)
            print(f"Total runtime: {strtotalruntime} seconds ({readable_duration})")
    
    print("Process completed")
except pymysql.MySQLError as e:
    print(f"❌ MySQL Error: {e}")
    cp.connectioncp.rollback()

