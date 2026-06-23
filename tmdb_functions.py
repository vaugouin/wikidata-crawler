import requests
import time
import json
from datetime import datetime
import pytz
import re
import os
from pathlib import Path
from dotenv import load_dotenv

# Import from citizenphil or define locally
import citizenphil as cp
from citizenphil import f_sqlupdatearray, f_stringtosql

load_dotenv(Path(__file__).resolve().with_name(".env"))

class _ConnectionProxy:
    def __getattr__(self, name):
        return getattr(cp.f_getconnection(), name)

connectioncp = _ConnectionProxy()

# Global variables
strtmdbapidomainurl = os.environ.get("TMDB_API_DOMAIN_URL", "")
strtmdbapitoken = os.environ.get("TMDB_API_TOKEN", "")
strsqlns = os.environ.get("DB_NAMESPACE", "")

headers = {
    "accept": "application/json",
    "Authorization": "Bearer " + strtmdbapitoken
}

paris_tz = pytz.timezone(os.environ.get("USER_TIMEZONE", "Europe/Paris"))
strdatepattern = r"^\d{4}-\d{2}-\d{2}$"
strlanguagecountry = "en-US"
strlanguage = "en"

# Selective TV refresh tuning (see SERIE_UPDATE.md). TODO: move to T_WC_SERVER_VARIABLE.
INT_RECENT_SEASON_DAYS = 120
INT_RECENT_EPISODE_DAYS = 45
INT_ACTIVE_SERIES_LOOKBACK_DAYS = 90
INT_TMDB_CHANGES_MAX_DAYS = 14

def f_tmdbjsonremovekeys(strjson,strbegin,strend,strreplace):
    """
    Remove a key-value section from a JSON string by finding and replacing text between markers.

    Parameters:
    -----------
    strjson : str
        The JSON string to process
    strbegin : str
        The starting marker to find (e.g., ', "overview":')
    strend : str
        The ending marker to find (e.g., ', "popularity":')
    strreplace : str
        The replacement string to insert

    Returns:
    --------
    str
        The modified JSON string with sections removed
    """
    # strbegin = ', "overview":'
    lnglenbegin = len(strbegin)
    # strend = ', "popularity":'
    lnglenend = len(strend)
    # strreplace = ', "popularity":'
    while True:
        lngposbegin = strjson.find(strbegin)
        if lngposbegin == -1:
            # Begin string not found
            break
        else:
            lngposend = strjson.find(strend, lngposbegin+lnglenbegin)
            if lngposend == -1:
                # End string not found
                break
            else:
                strjson = strjson[0:lngposbegin] + strreplace + strjson[lngposend + lnglenend:]
    return strjson

def f_tmdbfetchjson(strtmdbapifullurl, strcontext):
    intencore = True
    intattemptsremaining = 5
    response = None
    while intencore:
        try:
            response = requests.get(strtmdbapifullurl, headers=headers, timeout=30)
            if not response.text or response.text.strip() == "":
                raise ValueError("Empty response body")
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except requests.exceptions.ConnectionError as conn_err:
            print(f'Connection error occurred: {conn_err}')
        except requests.exceptions.Timeout as timeout_err:
            print(f'Timeout error occurred: {timeout_err}')
        except requests.exceptions.JSONDecodeError as json_err:
            print(f'JSON decode error occurred: {json_err}')
            if response is not None and response.text:
                print(f'TMDb raw response excerpt: {response.text[:200]}')
        except requests.exceptions.RequestException as req_err:
            print(f'Request error occurred: {req_err}')
        except ValueError as value_err:
            print(f'Value error occurred: {value_err}')
        except Exception as err:
            print(f'An error occurred: {err}')
        intattemptsremaining = intattemptsremaining - 1
        if intattemptsremaining > 0:
            time.sleep(1)
        else:
            intencore = False
    print(f"{strcontext} failed!")
    return None

def f_tmdbcontentimagesstosql(lngcontentid, strcontenttype, strsqlmastertable, strsqltablename, strkeyfieldname, strmainimagefield=None, strmainimagetype=None, strlangtable=None):
    """
    Fetch images for content from TMDb API and store them in the database.

    Parameters:
    -----------
    lngcontentid : int
        The TMDb content ID (movie, series, person, etc.)
    strcontenttype : str
        The type of content (e.g., 'movie', 'tv', 'person', 'collection')
    strsqlmastertable : str
        The main table name (e.g., 'T_WC_TMDB_MOVIE')
    strsqltablename : str
        The image table name (e.g., 'T_WC_TMDB_MOVIE_IMAGE')
    strkeyfieldname : str
        The primary key field name (e.g., 'ID_MOVIE')
    strmainimagefield : str, optional
        Name of the column holding the "main" image path (e.g., 'POSTER_PATH').
        When provided, every main image (the base/English image on the master
        record plus any per-language images, see strlangtable) is pinned to
        DISPLAY_ORDER 0, inserted if the API did not return it, and never
        deleted by the obsolete-image cleanup.
    strmainimagetype : str, optional
        TYPE_IMAGE value for the main image (e.g., 'poster'). Required when
        strmainimagefield is set.
    strlangtable : str, optional
        Name of the per-language table (e.g., 'T_WC_TMDB_MOVIE_LANG') that holds
        localized main image paths in the same strmainimagefield column, keyed by
        strkeyfieldname with a LANG column. Each localized main image is also
        pinned to DISPLAY_ORDER 0.

    Returns:
    --------
    bool
        True if successful, False if failed or invalid ID
    """
    global strtmdbapidomainurl
    global headers
    global connectioncp
    global strsqlns
    global paris_tz

    if lngcontentid <= 0:
        print(f"Error: Invalid {strcontenttype} ID {lngcontentid}")
        return False

    strtmdbapiimagesurl = f"3/{strcontenttype}/{lngcontentid}/images"
    strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapiimagesurl
    data = f_tmdbfetchjson(strtmdbapifullurl, f"f_tmdbcontentimagesstosql({lngcontentid})")
    if data is None:
        return False

    if 'status_code' in data and data['status_code'] > 1:
        print(f"Error: API returned status code {data['status_code']}")
        if 'status_message' in data:
            print(f"Status message: {data['status_message']}")
        return False
    
    # Get current timestamp for database records
    current_time = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
    current_date = datetime.now(paris_tz).strftime("%Y-%m-%d")

    # Gather every "main" image path that must sit at DISPLAY_ORDER 0: the
    # base/English image on the master record plus any localized images (e.g. the
    # French POSTER_PATH stored in the *_LANG table). Mapped to their language so
    # missing ones can be inserted with a sensible LANG value.
    dctmainimages = {}
    if strmainimagefield:
        cursormain = connectioncp.cursor()
        cursormain.execute(f"SELECT {strmainimagefield} AS MAIN_IMAGE_PATH FROM {strsqlmastertable} WHERE {strkeyfieldname} = {lngcontentid}")
        rowmain = cursormain.fetchone()
        if rowmain is not None and rowmain.get('MAIN_IMAGE_PATH'):
            dctmainimages[rowmain['MAIN_IMAGE_PATH']] = 'en'
        if strlangtable:
            cursormain.execute(f"SELECT {strmainimagefield} AS MAIN_IMAGE_PATH, LANG FROM {strlangtable} WHERE {strkeyfieldname} = {lngcontentid}")
            for rowlang in cursormain.fetchall():
                strlangpath = rowlang.get('MAIN_IMAGE_PATH')
                if strlangpath:
                    dctmainimages.setdefault(strlangpath, rowlang.get('LANG') or '')

    # Track all image paths to clean up obsolete ones later
    all_image_paths = []

    # Function to process image arrays (both backdrops and posters)
    def process_image_array(image_array, image_type):
        lngdisplayorder = 0
        boopintype = (bool(dctmainimages) and image_type == strmainimagetype)
        for image in image_array:
            # Extract image data
            image_path = image.get('file_path', '')
            if not image_path:
                continue

            # Every main image (base + per-language) is pinned to DISPLAY_ORDER 0;
            # all other images keep a 1-based ordering.
            boothismain = boopintype and image_path in dctmainimages
            if boothismain:
                lngthisdisplayorder = 0
            else:
                lngdisplayorder += 1
                lngthisdisplayorder = lngdisplayorder

            all_image_paths.append(image_path)

            # Prepare data for database
            arrimagedata = {
                strkeyfieldname: lngcontentid,
                "DISPLAY_ORDER": lngthisdisplayorder,
                "DAT_CREAT": current_date,
                "TIM_UPDATED": current_time,
                "TYPE_IMAGE": image_type,
                "LANG": image.get('iso_639_1', ''),
                "IMAGE_PATH": image_path,
                "ASPECT_RATIO": image.get('aspect_ratio', 0),
                "WIDTH": image.get('width', 0),
                "HEIGHT": image.get('height', 0),
                "VOTE_AVERAGE": image.get('vote_average', 0),
                "VOTE_COUNT": image.get('vote_count', 0)
            }
            # Keep the pinned main image active even if a prior run soft-deleted it.
            if boothismain:
                arrimagedata["DELETED"] = 0

            # Update or insert into database
            strsqlupdatecondition = f"{strkeyfieldname} = {lngcontentid} AND TYPE_IMAGE = '{image_type}' AND IMAGE_PATH = '{image_path}'"
            cp.f_sqlupdatearray(strsqltablename, arrimagedata, strsqlupdatecondition, 1)

    # Process backdrops
    if 'backdrops' in data and data['backdrops']:
        process_image_array(data['backdrops'], 'backdrop')
    
    # Process posters
    if 'posters' in data and data['posters']:
        process_image_array(data['posters'], 'poster')
    
    # Process logos
    if 'logos' in data and data['logos']:
        process_image_array(data['logos'], 'logo')
    
    # Process profiles
    if 'profiles' in data and data['profiles']:
        process_image_array(data['profiles'], 'profile')

    # Guarantee every main image (base + per-language) is present at
    # DISPLAY_ORDER 0, even when the TMDb images endpoint did not return it.
    # Adding them to all_image_paths also shields them from the cleanup below.
    for strmainpath, strmainlang in dctmainimages.items():
        if strmainpath in all_image_paths:
            continue
        all_image_paths.append(strmainpath)
        arrmainimagedata = {
            strkeyfieldname: lngcontentid,
            "DISPLAY_ORDER": 0,
            "DELETED": 0,
            "DAT_CREAT": current_date,
            "TIM_UPDATED": current_time,
            "TYPE_IMAGE": strmainimagetype,
            "IMAGE_PATH": strmainpath,
            "LANG": strmainlang,
        }
        strsqlupdatecondition = f"{strkeyfieldname} = {lngcontentid} AND TYPE_IMAGE = '{strmainimagetype}' AND IMAGE_PATH = '{strmainpath}'"
        cp.f_sqlupdatearray(strsqltablename, arrmainimagedata, strsqlupdatecondition, 1)

    # Clean up obsolete images
    if all_image_paths:
        # Create a comma-separated list of image paths with quotes
        image_paths_list = "'" + "', '".join(all_image_paths) + "'"
        
        # Delete images that are no longer present in the API response
        strsqldelete = f"DELETE FROM {strsqltablename} WHERE {strkeyfieldname} = {lngcontentid} AND IMAGE_PATH NOT IN ({image_paths_list})"
        cursor = connectioncp.cursor()
        cursor.execute(strsqldelete)
        connectioncp.commit()
    else:
        # If no images were found, delete all images for this contents
        strsqldelete = f"DELETE FROM {strsqltablename} WHERE {strkeyfieldname} = {lngcontentid}"
        cursor = connectioncp.cursor()
        cursor.execute(strsqldelete)
        connectioncp.commit()
    
    # Update the content record to mark images as completed
    strtimimagescompleted = current_time
    strsqlupdatecondition = f"{strkeyfieldname} = {lngcontentid}"
    strsqlupdatesetclause = f"TIM_IMAGES_COMPLETED = '{strtimimagescompleted}'"
    strsqlupdate = f"UPDATE {strsqlmastertable} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
    cursor = connectioncp.cursor()
    cursor.execute(strsqlupdate)
    connectioncp.commit()
    return True
    
def f_tmdbcontentvideosstosql(lngcontentid, strcontenttype, strsqlmastertable, strsqltablename, strkeyfieldname, strlang):
    """
    Fetch videos for content from TMDb API and store them in the database.

    Parameters:
    -----------
    lngcontentid : int
        The TMDb content ID (movie, series, etc.)
    strcontenttype : str
        The type of content (e.g., 'movie', 'tv')
    strsqlmastertable : str
        The main table name (e.g., 'T_WC_TMDB_MOVIE')
    strsqltablename : str
        The video table name (e.g., 'T_WC_TMDB_MOVIE_VIDEO')
    strkeyfieldname : str
        The primary key field name (e.g., 'ID_MOVIE')
    strlang : str
        The language code for videos (e.g., 'en', 'fr')

    Returns:
    --------
    bool
        True if successful, False if failed or invalid ID
    """
    global strtmdbapidomainurl
    global headers
    global connectioncp
    global strsqlns
    global paris_tz
    
    if lngcontentid <= 0:
        print(f"Error: Invalid {strcontenttype} ID {lngcontentid}")
        return False
    
    strtmdbapivideosurl = f"3/{strcontenttype}/{lngcontentid}/videos?language={strlang}"
    strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapivideosurl
    data = f_tmdbfetchjson(strtmdbapifullurl, f"f_tmdbcontentvideosstosql({lngcontentid}, {strlang})")
    if data is None:
        return False

    if 'status_code' in data and data['status_code'] > 1:
        print(f"Error: API returned status code {data['status_code']}")
        if 'status_message' in data:
            print(f"Status message: {data['status_message']}")
        return False
    
    # Get current timestamp for database records
    current_time = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
    current_date = datetime.now(paris_tz).strftime("%Y-%m-%d")
    
    # Track all video ids to clean up obsolete ones later
    all_video_ids = []
    
    lngdisplayorder = 0
    if 'results' in data and data['results']:
        for video in data['results']:
            lngdisplayorder += 1
            
            # Extract video data
            video_id = video.get('id', '')
            if not video_id:
                continue
                
            all_video_ids.append(video_id)
            
            # Parse published_at datetime if available
            dat_published = None
            published_at_str = video.get('published_at')
            if published_at_str:
                try:
                    # Handle common TMDb ISO 8601 format with trailing 'Z'
                    if published_at_str.endswith('Z'):
                        dt_utc = datetime.strptime(published_at_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.utc)
                    else:
                        dt_utc = datetime.fromisoformat(published_at_str)
                        if dt_utc.tzinfo is None:
                            dt_utc = dt_utc.replace(tzinfo=pytz.utc)
                    dt_local = dt_utc.astimezone(paris_tz)
                    dat_published = dt_local.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    dat_published = None

            # Prepare data for database
            arrvideodata = {
                strkeyfieldname: lngcontentid,
                "DISPLAY_ORDER": lngdisplayorder,
                "DAT_CREAT": current_date,
                "TIM_UPDATED": current_time,
                "DAT_PUBLISHED": dat_published,
                "VIDEO_TYPE": video.get('type', ''),
                "LANG": video.get('iso_639_1', ''),
                "COUNTRY_CODE": video.get('iso_3166_1', ''),
                "ID_CREDIT": video_id,
                "VIDEO_KEY": video.get('key', ''),
                "VIDEO_NAME": video.get('name', ''),
                "VIDEO_SITE": video.get('site', ''),
                "QUALITY": video.get('size', 0),
                "QUALITY_TEXT": str(video.get('size', 0)) + 'p',
                "OFFICIAL": video.get('official', False)
            }
            #print(arrvideodata)
            # Update or insert into database
            strsqlupdatecondition = f"{strkeyfieldname} = {lngcontentid} AND LANG = '{strlang}' AND ID_CREDIT = '{video_id}'"
            #print(strsqlupdatecondition)
            cp.f_sqlupdatearray(strsqltablename, arrvideodata, strsqlupdatecondition, 1)
    
    # Clean up obsolete videos
    if all_video_ids:
        # Create a comma-separated list of video ids with quotes
        video_ids_list = "'" + "', '".join(all_video_ids) + "'"
        
        # Delete videos that are no longer present in the API response
        strsqldelete = f"DELETE FROM {strsqltablename} WHERE {strkeyfieldname} = {lngcontentid} AND LANG = '{strlang}' AND ID_CREDIT NOT IN ({video_ids_list})"
        cursor = connectioncp.cursor()
        cursor.execute(strsqldelete)
        connectioncp.commit()
    else:
        # If no videos were found, delete all videos for this content and language 
        strsqldelete = f"DELETE FROM {strsqltablename} WHERE {strkeyfieldname} = {lngcontentid} AND LANG = '{strlang}'"
        cursor = connectioncp.cursor()
        cursor.execute(strsqldelete)
        connectioncp.commit()
    
    # Update the content record to mark videos as completed
    strtimvideoscompleted = current_time
    strsqlupdatecondition = f"{strkeyfieldname} = {lngcontentid}"
    strsqlupdatesetclause = f"TIM_VIDEOS_COMPLETED = '{strtimvideoscompleted}'"
    strsqlupdate = f"UPDATE {strsqlmastertable} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
    cursor = connectioncp.cursor()
    cursor.execute(strsqlupdate)
    connectioncp.commit()
    return True

# https://developer.themoviedb.org/reference/person-details

def f_tmdbpersontosql(lngpersonid):
    """
    Fetch person details from TMDb API and store in database.

    Parameters:
    -----------
    lngpersonid : int
        The TMDb person ID to fetch

    Returns:
    --------
    bool
        True if successful, False if failed or invalid ID
    """
    global strtmdbapidomainurl
    global headers
    global strdatepattern

    if lngpersonid > 0:
        strtmdbapipersonurl = "3/person/" + str(lngpersonid) + "?append_to_response=combined_credits,external_ids"
        strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapipersonurl
        # print(strtmdbapifullurl)
        data = f_tmdbfetchjson(strtmdbapifullurl, f"f_tmdbpersontosql({lngpersonid})")
        if data is None:
            return
        else:
            # strapiperson = response.text
            # Parse the JSON data into a dictionary
            
            lngpersonstatuscode = 0
            if 'status_code' in data:
                lngpersonstatuscode = data['status_code']
            if lngpersonstatuscode <= 1:
                # API request result is not an error
                # Extract data using the keys
                strpersonidimdb = ""
                if 'imdb_id' in data:
                    strpersonidimdb = data['imdb_id']
                strpersonbiography = data['biography']
                strpersonbirthday = data['birthday']
                lngpersonbirthyear = None
                lngpersonbirthmonth = None
                lngpersonbirthday = None
                if strpersonbirthday:
                    if not re.match(strdatepattern, strpersonbirthday):
                        # Date is invalid
                        strpersonbirthday = None
                    else:
                        lngpersonbirthyear, lngpersonbirthmonth, lngpersonbirthday = map(int, strpersonbirthday.split('-'))
                strpersondeathday = data['deathday']
                lngpersondeathyear = None
                lngpersondeathmonth = None
                lngpersondeathday = None
                if strpersondeathday:
                    if not re.match(strdatepattern, strpersondeathday):
                        # Date is invalid
                        strpersondeathday = None
                    else:
                        lngpersondeathyear, lngpersondeathmonth, lngpersondeathday = map(int, strpersondeathday.split('-'))
                intpersongender = data['gender']
                strpersonprofilepath = data['profile_path']
                strpersonhomepage = data['homepage']
                strpersonname = data['name']
                strpersonplaceofbirth = str(data['place_of_birth'])
                strpersonplaceofbirth = strpersonplaceofbirth.strip()
                if strpersonplaceofbirth: 
                    if len(strpersonplaceofbirth) > 200:
                        # If place of birth is too long, we chop it
                        strpersonplaceofbirth = strpersonplaceofbirth[:200]
                dblpersonpopularity = data['popularity']
                strpersonknownfordepartment = data['known_for_department']
                boopersonadult = data['adult']
                intpersonadult = 0
                if boopersonadult:
                    intpersonadult = 1
                arrpersonalsoknownas = data['also_known_as']
                strpersonalsoknownas = "|"
                for strvalue in arrpersonalsoknownas:
                    strpersonalsoknownas += strvalue + "|"
                
                strpersonidwikidata = ""
                if 'external_ids' in data:
                    if 'wikidata_id' in data['external_ids']:
                        strpersonidwikidata = data['external_ids']['wikidata_id']
                
                # print(f"{strpersonname} {strpersonidimdb}")
                #"print(f"{strpersonknownfordepartment}")
                # if strpersonbirthday:
                #     print(f"Birth {strpersonbirthday}")
                # if strpersondeathday: 
                #     print(f"Death {strpersondeathday}")
                # print(f"Biography: {strpersonbiography}")
                # print(f"Adult: {intpersonadult}")
                # print(f"Also know as: {strpersonalsoknownas}")
                # print(f"Popularity: {dblpersonpopularity}")
                
                if strpersonhomepage: 
                    if len(strpersonhomepage) > 250:
                        # If homepage URL is too long, we chop it
                        strpersonhomepage = strpersonhomepage[:250]
                
                arrpersoncouples = {}
                #arrpersoncouples["API_URL"] = strtmdbapipersonurl
                # arrpersoncouples["API_RESULT"] = strapipersonfordb
                if strpersonidimdb:
                    arrpersoncouples["ID_IMDB"] = strpersonidimdb
                else:
                    arrpersoncouples["ID_IMDB"] = ""
                if strpersonidwikidata:
                    arrpersoncouples["ID_WIKIDATA"] = strpersonidwikidata
                else:
                    arrpersoncouples["ID_WIKIDATA"] = ""
                arrpersoncouples["BIOGRAPHY"] = strpersonbiography
                if strpersonbirthday:
                    arrpersoncouples["BIRTHDAY"] = strpersonbirthday
                arrpersoncouples["BIRTH_YEAR"] = lngpersonbirthyear
                arrpersoncouples["BIRTH_MONTH"] = lngpersonbirthmonth
                arrpersoncouples["BIRTH_DAY"] = lngpersonbirthday
                if strpersondeathday: 
                    arrpersoncouples["DEATHDAY"] = strpersondeathday
                arrpersoncouples["DEATH_YEAR"] = lngpersondeathyear
                arrpersoncouples["DEATH_MONTH"] = lngpersondeathmonth
                arrpersoncouples["DEATH_DAY"] = lngpersondeathday
                arrpersoncouples["GENDER"] = intpersongender
                arrpersoncouples["ID_PERSON"] = lngpersonid
                arrpersoncouples["PROFILE_PATH"] = strpersonprofilepath
                arrpersoncouples["HOMEPAGE_URL"] = strpersonhomepage
                arrpersoncouples["NAME"] = strpersonname
                arrpersoncouples["PLACE_OF_BIRTH"] = strpersonplaceofbirth
                arrpersoncouples["POPULARITY"] = dblpersonpopularity
                arrpersoncouples["KNOWN_FOR_DEPARTMENT"] = strpersonknownfordepartment
                arrpersoncouples["ADULT"] = intpersonadult
                arrpersoncouples["ALSO_KNOWN_AS"] = strpersonalsoknownas
                
                strmoviecredits = ""
                strseriecredits = ""
                arrcredittype = {1: 'cast', 2:'crew'}
                for intcredittype,strpersoncreditcredittype in arrcredittype.items():
                    if intcredittype == 1:
                        strtitle = "Cast"
                    else:
                        strtitle = "Crew"
                    # print(strtitle)
                    if "combined_credits" in data:
                        if strpersoncreditcredittype in data['combined_credits']:
                            arrpersoncredits = data['combined_credits'][strpersoncreditcredittype]
                            # print(arrpersoncredits)
                            for onecontent in arrpersoncredits:
                                strpersoncreditmediatype = onecontent['media_type']
                                # print(strpersoncreditmediatype)
                                if strpersoncreditmediatype == "movie":
                                    # This is a movie
                                    strpersoncredittitle = onecontent['title']
                                    strpersoncreditreleaseyear = ""
                                    if 'release_date' in onecontent:
                                        strpersoncreditreleasedate = onecontent['release_date']
                                        strpersoncreditreleaseyear = strpersoncreditreleasedate[:4]
                                    strpersoncreditcreditid = onecontent['credit_id']
                                    lngmovieid = onecontent['id']
                                    arrpersonmoviecouples = {}
                                    if intcredittype == 1:
                                        strpersoncreditcharacter = onecontent['character']
                                        strpersoncreditdepartment = ""
                                        strpersoncreditjob = ""
                                        # print(f"{strpersoncredittitle} ({strpersoncreditreleaseyear}): {strpersoncreditcharacter}")
                                    else:
                                        strpersoncreditcharacter = ""
                                        strpersoncreditdepartment = onecontent['department']
                                        strpersoncreditjob = onecontent['job']
                                        # print(f"{strpersoncredittitle} ({strpersoncreditreleaseyear}): {strpersoncreditdepartment} {strpersoncreditjob}")
                                    if strmoviecredits != "":
                                        strmoviecredits += ","
                                    strmoviecredits += "'" + strpersoncreditcreditid + "'"
                                    arrpersonmoviecouples["ID_PERSON"] = lngpersonid
                                    arrpersonmoviecouples["ID_MOVIE"] = lngmovieid
                                    arrpersonmoviecouples["ID_CREDIT"] = strpersoncreditcreditid
                                    arrpersonmoviecouples["CAST_CHARACTER"] = strpersoncreditcharacter
                                    arrpersonmoviecouples["CREW_DEPARTMENT"] = strpersoncreditdepartment
                                    arrpersonmoviecouples["CREW_JOB"] = strpersoncreditjob
                                    arrpersonmoviecouples["CREDIT_TYPE"] = strpersoncreditcredittype
                                    # print(arrpersonmoviecouples)
                                    
                                    strsqltablename = "T_WC_TMDB_PERSON_MOVIE"
                                    strsqlupdatecondition = f"ID_CREDIT = '{strpersoncreditcreditid}'"
                                    cp.f_sqlupdatearray(strsqltablename,arrpersonmoviecouples,strsqlupdatecondition,1)
                                    
                                elif strpersoncreditmediatype == "tv":
                                    # This is a TV show
                                    strpersoncredittitle = onecontent['name']
                                    strpersoncreditreleaseyear = ""
                                    if 'first_air_date' in onecontent:
                                        strpersoncreditreleasedate = onecontent['first_air_date']
                                        strpersoncreditreleaseyear = strpersoncreditreleasedate[:4]
                                    strpersoncreditcreditid = onecontent['credit_id']
                                    lngserieid = onecontent['id']
                                    arrpersonseriecouples = {}
                                    if intcredittype == 1:
                                        strpersoncreditcharacter = onecontent['character']
                                        strpersoncreditdepartment = ""
                                        strpersoncreditjob = ""
                                        # print(f"{strpersoncredittitle} ({strpersoncreditreleaseyear}): {strpersoncreditcharacter}")
                                    else:
                                        strpersoncreditcharacter = ""
                                        strpersoncreditdepartment = onecontent['department']
                                        strpersoncreditjob = onecontent['job']
                                        # print(f"{strpersoncredittitle} ({strpersoncreditreleaseyear}): {strpersoncreditdepartment} {strpersoncreditjob}")
                                    if strseriecredits != "":
                                        strseriecredits += ","
                                    strseriecredits += "'" + strpersoncreditcreditid + "'"
                                    arrpersonseriecouples["ID_PERSON"] = lngpersonid
                                    arrpersonseriecouples["ID_SERIE"] = lngserieid
                                    arrpersonseriecouples["ID_CREDIT"] = strpersoncreditcreditid
                                    arrpersonseriecouples["CAST_CHARACTER"] = strpersoncreditcharacter
                                    arrpersonseriecouples["CREW_DEPARTMENT"] = strpersoncreditdepartment
                                    arrpersonseriecouples["CREW_JOB"] = strpersoncreditjob
                                    arrpersonseriecouples["CREDIT_TYPE"] = strpersoncreditcredittype
                                    # print(arrpersonseriecouples)
                                    
                                    strsqltablename = "T_WC_TMDB_PERSON_SERIE"
                                    strsqlupdatecondition = f"ID_CREDIT = '{strpersoncreditcreditid}'"
                                    cp.f_sqlupdatearray(strsqltablename,arrpersonseriecouples,strsqlupdatecondition,1)
                
                encoded_biography = data['biography'].replace('\n', '\\n').replace('"', '\\"')
                data['biography'] = encoded_biography
                strapipersonfordb = json.dumps(data, ensure_ascii=False)
                strapipersonfordb = f_tmdbjsonremovekeys(strapipersonfordb,', "overview":',', "popularity":',', "popularity":')
                strapipersonfordb = f_tmdbjsonremovekeys(strapipersonfordb,', "original_title":',', "popularity":',', "popularity":')
                strapipersonfordb = f_tmdbjsonremovekeys(strapipersonfordb,', "title":',', "video":',', "video":')
                #arrpersoncouples["API_RESULT"] = strapipersonfordb
                #arrpersoncouples["CRAWLER_VERSION"] = 3
                
                strsqltablename = "T_WC_TMDB_PERSON"
                strsqlupdatecondition = f"ID_PERSON = {lngpersonid}"
                cp.f_sqlupdatearray(strsqltablename,arrpersoncouples,strsqlupdatecondition,1)
                
                # Now delete credits that are not for this person
                if strmoviecredits == "":
                    strmoviecredits = "'0'"
                strsqldelete = "DELETE FROM T_WC_TMDB_PERSON_MOVIE WHERE ID_PERSON = " + str(lngpersonid) + " AND ID_CREDIT NOT IN (" + strmoviecredits + ")"
                # print(f"{strsqldelete}")
                cursor2 = connectioncp.cursor()
                cursor2.execute(strsqldelete)
                connectioncp.commit()
                if strseriecredits == "":
                    strseriecredits = "'0'"
                strsqldelete = "DELETE FROM T_WC_TMDB_PERSON_SERIE WHERE ID_PERSON = " + str(lngpersonid) + " AND ID_CREDIT NOT IN (" + strseriecredits + ")"
                # print(f"{strsqldelete}")
                cursor2 = connectioncp.cursor()
                cursor2.execute(strsqldelete)
                connectioncp.commit()

def f_tmdbpersonexist(lngpersonid):
    """
    Check if a person exists in the TMDb API.

    Parameters:
    -----------
    lngpersonid : int
        The TMDb person ID to check

    Returns:
    --------
    bool
        True if person exists, False if not found (status_code 34)
    """
    global strtmdbapidomainurl
    global headers
    global strlanguagecountry
    global strlanguage

    # By default, we assume that this person exists
    intresult = True
    if lngpersonid > 0:
        strtmdbapipersonurl = "3/person/" + str(lngpersonid) + "?language=" + strlanguage
        strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapipersonurl
        # print(strtmdbapifullurl)
        data = f_tmdbfetchjson(strtmdbapifullurl, f"f_tmdbpersonexists({lngpersonid})")
        if data is None:
            return intresult
        lngpersonstatuscode = 0
        if 'status_code' in data:
            lngpersonstatuscode = data['status_code']
        if lngpersonstatuscode == 34:
            # API request result is an error:
            # The resource you requested could not be found
            intresult = False
    return intresult

def f_tmdbpersondelete(lngpersonid):
    """
    Delete a person and all related records from the database.

    Parameters:
    -----------
    lngpersonid : int
        The TMDb person ID to delete

    Returns:
    --------
    None
    """
    global connectioncp

    if lngpersonid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_PERSON"
        strsqlupdatecondition = f"ID_PERSON = {lngpersonid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
        strsqltablename = "T_WC_TMDB_PERSON_MOVIE"
        strsqlupdatecondition = f"ID_PERSON = {lngpersonid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
        strsqltablename = "T_WC_TMDB_PERSON_SERIE"
        strsqlupdatecondition = f"ID_PERSON = {lngpersonid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
        strsqltablename = "T_WC_TMDB_PERSON_IMAGE"
        strsqlupdatecondition = f"ID_PERSON = {lngpersonid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
def f_tmdbpersonsetcreditscompleted(lngpersonid):
    """
    Mark a person's credits as fully processed by setting TIM_CREDITS_COMPLETED timestamp.

    Parameters:
    -----------
    lngpersonid : int
        The TMDb person ID to update

    Returns:
    --------
    None
    """
    global paris_tz
    global connectioncp

    if lngpersonid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_PERSON"
        strsqlupdatecondition = f"ID_PERSON = {lngpersonid}"
        strtimcreditscompleted = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        strsqlupdatesetclause = f"TIM_CREDITS_COMPLETED = '{strtimcreditscompleted}', TIM_UPDATED = '{strtimcreditscompleted}'"
        strsqlupdate = f"UPDATE {strsqltablename} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdbpersonsetwikidatacompleted(lngpersonid):
    """
    Mark a person's Wikidata integration as completed by setting TIM_WIKIDATA_COMPLETED timestamp.

    Parameters:
    -----------
    lngpersonid : int
        The TMDb person ID to update

    Returns:
    --------
    None
    """
    global paris_tz
    global connectioncp

    if lngpersonid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_PERSON"
        strsqlupdatecondition = f"ID_PERSON = {lngpersonid}"
        strtimwikidatacompleted = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        strsqlupdatesetclause = f"TIM_WIKIDATA_COMPLETED = '{strtimwikidatacompleted}'"
        strsqlupdate = f"UPDATE {strsqltablename} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdbpersonimagestosql(lngpersonid):
    """
    Fetch and store images for a person from TMDb API.

    Parameters:
    -----------
    lngpersonid : int
        The TMDb person ID to fetch images for

    Returns:
    --------
    bool
        True if successful, False if failed
    """
    f_tmdbcontentimagesstosql(lngpersonid, "person", "T_WC_TMDB_PERSON", "T_WC_TMDB_PERSON_IMAGE", "ID_PERSON", "PROFILE_PATH", "profile")

def f_tmdbpersontosqleverything(lngpersonid):
    """
    Fetch and store complete person data including details, credits, and images.

    Parameters:
    -----------
    lngpersonid : int
        The TMDb person ID to fetch

    Returns:
    --------
    None
    """
    f_tmdbpersontosql(lngpersonid)
    f_tmdbpersonsetcreditscompleted(lngpersonid)
    f_tmdbpersonimagestosql(lngpersonid)

# https://developer.themoviedb.org/reference/movie-details

def f_tmdbmovietosql(lngmovieid):
    """
    Fetch movie details from TMDb API and store in database.

    Parameters:
    -----------
    lngmovieid : int
        The TMDb movie ID to fetch

    Returns:
    --------
    bool
        True if successful, False if failed or invalid ID
    """
    global strtmdbapidomainurl
    global headers
    global strlanguagecountry
    global strlanguage
    global strdatepattern
    global connectioncp
    global strsqlns

    if lngmovieid > 0:
        # New TMDb API call with append_to_response since 2024-05-24 10:00
        strtmdbapimovieurl = "3/movie/" + str(lngmovieid) + "?append_to_response=credits,alternative_titles,external_ids&language=" + strlanguage
        strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapimovieurl
        # print(strtmdbapifullurl)
        data = f_tmdbfetchjson(strtmdbapifullurl, f"f_tmdbmovietosql({lngmovieid})")
        if data is None:
            return

        lngmoviestatuscode = 0
        if 'status_code' in data:
            lngmoviestatuscode = data['status_code']
        if lngmoviestatuscode <= 1:
            # API request result is not an error
            # Extract data using the keys
            strmovieidimdb = ""
            if 'imdb_id' in data:
                strmovieidimdb = data['imdb_id']
            strmovieoverview = ""
            if 'overview' in data:
                strmovieoverview = data['overview']
            strmoviereleasedate = ""
            #strmoviereleaseyear = ""
            lngmoviereleaseyear = None
            lngmoviereleasemonth = None
            lngmoviereleaseday = None
            if 'release_date' in data:
                strmoviereleasedate = data['release_date']
                if not re.match(strdatepattern, strmoviereleasedate):
                    # Date is invalid
                    strmoviereleasedate = None
                else:
                    lngmoviereleaseyear, lngmoviereleasemonth, lngmoviereleaseday = map(int, strmoviereleasedate.split('-'))
            intmovievideo = 0
            if 'video' in data:
                boomovievideo = data['video']
                if boomovievideo:
                    intmovievideo = 1
            strmovieposterpath = ""
            if 'poster_path' in data:
                strmovieposterpath = data['poster_path']
            strmoviehomepage = ""
            if 'homepage' in data:
                strmoviehomepage = data['homepage']
            strmovietitle = ""
            if 'title' in data:
                strmovietitle = data['title']
            strmovieoriginallanguage = ""
            if 'original_language' in data:
                strmovieoriginallanguage = data['original_language']
            dblmoviepopularity = 0
            if 'popularity' in data:
                dblmoviepopularity = data['popularity']
            strmoviebackdroppath = ""
            if 'backdrop_path' in data:
                strmoviebackdroppath = data['backdrop_path']
            intmovieadult = 0
            if 'adult' in data:
                boomovieadult = data['adult']
                if boomovieadult:
                    intmovieadult = 1
            strmovieoriginaltitle = ""
            if 'original_title' in data:
                strmovieoriginaltitle = data['original_title']
                if strmovieoriginaltitle: 
                    if len(strmovieoriginaltitle) > 250:
                        strmovieoriginaltitle = strmovieoriginaltitle[:250]
            strmoviestatus = ""
            if 'status' in data:
                strmoviestatus = data['status']
            strmoviegenres = ""
            intdocumentary = False
            if 'genres' in data:
                arrmoviegenres = data['genres']
                strmoviegenres = "|"
                arrgenrenames = [genre['name'] for genre in arrmoviegenres]
                for strvalue in arrgenrenames:
                    strmoviegenres += strvalue + "|"
                    if strvalue == "Documentary":
                        # This is a documentary
                        intdocumentary = True
            lngcollectionid = 0
            if 'belongs_to_collection' in data:
                if data['belongs_to_collection']:
                    if 'id' in data['belongs_to_collection']:
                        lngcollectionid = data['belongs_to_collection']['id']
            dblmoviebudget = 0
            if 'budget' in data:
                dblmoviebudget = data['budget']
            lngmovieruntime = 0
            intmovieisshortfilm = 0
            if 'runtime' in data:
                lngmovieruntime = data['runtime']
                if lngmovieruntime > 58:
                    intmovieisshortfilm = 0
                elif lngmovieruntime > 0:
                    intmovieisshortfilm = 1
            dblmovierevenue = 0
            if 'revenue' in data:
                dblmovierevenue = data['revenue']
            strmovietagline = ""
            if 'tagline' in data:
                strmovietagline = data['tagline']
            dblmovievoteaverage = 0
            if 'vote_average' in data:
                dblmovievoteaverage = data['vote_average']
            lngmovievotecount = 0
            if 'vote_count' in data:
                lngmovievotecount = data['vote_count']
            strmovieidwikidata = ""
            if 'external_ids' in data:
                if 'wikidata_id' in data['external_ids']:
                    strmovieidwikidata = data['external_ids']['wikidata_id']
            strmoviecountries = ""
            strcountryidlist = ""
            lngcountrydisplayorder = 0
            if 'production_countries' in data:
                arrmoviecountries = data['production_countries']
                strmoviecountries = "|"
                arrcountrynames = [country['iso_3166_1'] for country in arrmoviecountries]
                for strvalue in arrcountrynames:
                    strmoviecountries += strvalue + "|"
                    lngcountrydisplayorder = lngcountrydisplayorder + 1
                    if strcountryidlist != "":
                        strcountryidlist += ","
                    strcountryidlist += "'" + strvalue + "'"
                    arrmoviecountrycouples = {}
                    arrmoviecountrycouples["ID_MOVIE"] = lngmovieid
                    arrmoviecountrycouples["COUNTRY_CODE"] = strvalue
                    arrmoviecountrycouples["DISPLAY_ORDER"] = lngcountrydisplayorder
                    # print(arrmoviecountrycouples)
                    strsqltablename = "T_WC_TMDB_MOVIE_PRODUCTION_COUNTRY"
                    strsqlupdatecondition = f"ID_MOVIE = {lngmovieid} AND COUNTRY_CODE = '{strvalue}'"
                    cp.f_sqlupdatearray(strsqltablename,arrmoviecountrycouples,strsqlupdatecondition,1)
            if strcountryidlist != "":
                strsqldelete = "DELETE FROM " + strsqlns + "TMDB_MOVIE_PRODUCTION_COUNTRY WHERE ID_MOVIE = " + str(lngmovieid) + " AND COUNTRY_CODE NOT IN (" + strcountryidlist + ") "
                cursor2 = connectioncp.cursor()
                cursor2.execute(strsqldelete)
                connectioncp.commit()
            
            strmoviespokenlanguages = ""
            strspokenlanguageidlist = ""
            lngspokenlanguagedisplayorder = 0
            if 'spoken_languages' in data:
                arrmoviespokenlanguages = data['spoken_languages']
                strmoviespokenlanguages = "|"
                arrspokenlanguagenames = [spokenlanguage['iso_639_1'] for spokenlanguage in arrmoviespokenlanguages]
                for strvalue in arrspokenlanguagenames:
                    strmoviespokenlanguages += strvalue + "|"
                    lngspokenlanguagedisplayorder = lngspokenlanguagedisplayorder + 1
                    if strspokenlanguageidlist != "":
                        strspokenlanguageidlist += ","
                    strspokenlanguageidlist += "'" + strvalue + "'"
                    arrmoviespokenlanguagecouples = {}
                    arrmoviespokenlanguagecouples["ID_MOVIE"] = lngmovieid
                    arrmoviespokenlanguagecouples["SPOKEN_LANGUAGE"] = strvalue
                    arrmoviespokenlanguagecouples["DISPLAY_ORDER"] = lngspokenlanguagedisplayorder
                    # print(arrmoviespokenlanguagecouples)
                    strsqltablename = "T_WC_TMDB_MOVIE_SPOKEN_LANGUAGE"
                    strsqlupdatecondition = f"ID_MOVIE = {lngmovieid} AND SPOKEN_LANGUAGE = '{strvalue}'"
                    cp.f_sqlupdatearray(strsqltablename,arrmoviespokenlanguagecouples,strsqlupdatecondition,1)
            if strspokenlanguageidlist != "":
                strsqldelete = "DELETE FROM " + strsqlns + "TMDB_MOVIE_SPOKEN_LANGUAGE WHERE ID_MOVIE = " + str(lngmovieid) + " AND SPOKEN_LANGUAGE NOT IN (" + strspokenlanguageidlist + ") "
                cursor2 = connectioncp.cursor()
                cursor2.execute(strsqldelete)
                connectioncp.commit()
            
            strcompanyidlist = ""
            lngcompanydisplayorder = 0
            if data['production_companies']:
                # Array is not empty
                for onecontent in data['production_companies']:
                    lngcompanyid = onecontent['id']
                    lngcompanydisplayorder = lngcompanydisplayorder + 1
                    if strcompanyidlist != "":
                        strcompanyidlist += ","
                    strcompanyidlist += str(lngcompanyid)
                    arrmoviecompanycouples = {}
                    arrmoviecompanycouples["ID_MOVIE"] = lngmovieid
                    arrmoviecompanycouples["ID_COMPANY"] = lngcompanyid
                    arrmoviecompanycouples["DISPLAY_ORDER"] = lngcompanydisplayorder
                    # print(arrmoviecompanycouples)
                    strsqltablename = "T_WC_TMDB_MOVIE_COMPANY"
                    strsqlupdatecondition = f"ID_MOVIE = {lngmovieid} AND ID_COMPANY = {lngcompanyid}"
                    cp.f_sqlupdatearray(strsqltablename,arrmoviecompanycouples,strsqlupdatecondition,1)
            if strcompanyidlist != "":
                strsqldelete = "DELETE FROM " + strsqlns + "TMDB_MOVIE_COMPANY WHERE ID_MOVIE = " + str(lngmovieid) + " AND ID_COMPANY NOT IN (" + strcompanyidlist + ") "
                cursor2 = connectioncp.cursor()
                cursor2.execute(strsqldelete)
                connectioncp.commit()
            
            strgenreidlist = ""
            lnggenredisplayorder = 0
            if data['genres']:
                # Array is not empty
                for onecontent in data['genres']:
                    lnggenreid = onecontent['id']
                    lnggenredisplayorder = lnggenredisplayorder + 1
                    if strgenreidlist != "":
                        strgenreidlist += ","
                    strgenreidlist += str(lnggenreid)
                    arrmoviegenrecouples = {}
                    arrmoviegenrecouples["ID_MOVIE"] = lngmovieid
                    arrmoviegenrecouples["ID_GENRE"] = lnggenreid
                    arrmoviegenrecouples["DISPLAY_ORDER"] = lnggenredisplayorder
                    # print(arrmoviegenrecouples)
                    
                    strsqltablename = "T_WC_TMDB_MOVIE_GENRE"
                    strsqlupdatecondition = f"ID_MOVIE = {lngmovieid} AND ID_GENRE = {lnggenreid}"
                    cp.f_sqlupdatearray(strsqltablename,arrmoviegenrecouples,strsqlupdatecondition,1)
            if strgenreidlist != "":
                strsqldelete = "DELETE FROM " + strsqlns + "TMDB_MOVIE_GENRE WHERE ID_MOVIE = " + str(lngmovieid) + " AND ID_GENRE NOT IN (" + strgenreidlist + ") "
                cursor2 = connectioncp.cursor()
                cursor2.execute(strsqldelete)
                connectioncp.commit()
            
            # print(f"{strmovietitle} ({strmoviereleaseyear}) {strmovieidimdb}")
            # print(f"{strmoviebackdroppath}")
            # if strmoviereleasedate:
            #     print(f"Release {strmoviereleasedate}")
            # print(f"Overview: {strmovieoverview}")
            # print(f"Adult: {intmovieadult}")
            # print(f"Original title: {strmovieoriginaltitle}")
            # print(f"Popularity: {dblmoviepopularity}")
            
            if strmovietitle:
                if len(strmovietitle) > 250:
                    # If title is too long, we chop it
                    strmovietitle = strmovietitle[:250]
            if strmoviehomepage:
                if len(strmoviehomepage) > 500:
                    # If homepage URL is too long, we chop it
                    strmoviehomepage = strmoviehomepage[:500]
            if intdocumentary:
                intisdocumentary = 1
                intismovie = 0
            else:
                intisdocumentary = 0
                intismovie = 1
            
            arrmoviecouples = {}
            #arrmoviecouples["API_URL"] = strtmdbapimovieurl
            # arrmoviecouples["API_RESULT"] = strapimoviefordb
            if strmovieidimdb:
                arrmoviecouples["ID_IMDB"] = strmovieidimdb
            else:
                arrmoviecouples["ID_IMDB"] = ""
            if strmovieidwikidata:
                arrmoviecouples["ID_WIKIDATA"] = strmovieidwikidata
            else:
                arrmoviecouples["ID_WIKIDATA"] = ""
            arrmoviecouples["OVERVIEW"] = strmovieoverview
            if strmoviereleasedate:
                if strmoviereleasedate != "":
                    arrmoviecouples["DAT_RELEASE"] = strmoviereleasedate
            arrmoviecouples["RELEASE_YEAR"] = lngmoviereleaseyear
            arrmoviecouples["RELEASE_MONTH"] = lngmoviereleasemonth
            arrmoviecouples["RELEASE_DAY"] = lngmoviereleaseday
            arrmoviecouples["VIDEO"] = intmovievideo
            arrmoviecouples["ID_MOVIE"] = lngmovieid
            arrmoviecouples["POSTER_PATH"] = strmovieposterpath
            arrmoviecouples["HOMEPAGE_URL"] = strmoviehomepage
            if strmovietitle != "":
                arrmoviecouples["TITLE"] = strmovietitle
            else:
                arrmoviecouples["TITLE"] = ""
            arrmoviecouples["ORIGINAL_LANGUAGE"] = strmovieoriginallanguage
            arrmoviecouples["POPULARITY"] = dblmoviepopularity
            arrmoviecouples["BACKDROP_PATH"] = strmoviebackdroppath
            arrmoviecouples["ORIGINAL_TITLE"] = strmovieoriginaltitle
            arrmoviecouples["STATUS"] = strmoviestatus
            arrmoviecouples["GENRES"] = strmoviegenres
            arrmoviecouples["ID_COLLECTION"] = lngcollectionid
            arrmoviecouples["ADULT"] = intmovieadult
            arrmoviecouples["BUDGET"] = dblmoviebudget
            arrmoviecouples["RUNTIME"] = lngmovieruntime
            arrmoviecouples["IS_SHORT_FILM"] = intmovieisshortfilm
            arrmoviecouples["REVENUE"] = dblmovierevenue
            arrmoviecouples["TAGLINE"] = strmovietagline
            arrmoviecouples["VOTE_AVERAGE"] = dblmovievoteaverage
            arrmoviecouples["VOTE_COUNT"] = lngmovievotecount
            arrmoviecouples["COUNTRIES"] = strmoviecountries
            arrmoviecouples["SPOKEN_LANGUAGES"] = strmoviespokenlanguages
            arrmoviecouples["IS_DOCUMENTARY"] = intisdocumentary
            arrmoviecouples["IS_MOVIE"] = intismovie
            
            strpersoncredits = ""
            arrcredittype = {1: 'cast', 2:'crew'}
            for intcredittype,strmoviecreditcredittype in arrcredittype.items():
                if intcredittype == 1:
                    strtitle = "Cast"
                else:
                    strtitle = "Crew"
                # print(strtitle)
                if 'credits' in data:
                    if strmoviecreditcredittype in data['credits']:
                        arrmoviecredits = data['credits'][strmoviecreditcredittype]
                        lngdisplayorder = 0
                        arrcredits = {}
                        if arrmoviecredits:
                            # print(arrmoviecredits)
                            for onecontent in arrmoviecredits:
                                lngdisplayorder += 1
                                strpersoncreditname = onecontent['name']
                                strpersoncreditcreditid = onecontent['credit_id']
                                lngpersonid = onecontent['id']
                                if lngpersonid in arrcredits:
                                    lngcreditdisplayorder = arrcredits[lngpersonid]
                                else:
                                    lngcreditdisplayorder = lngdisplayorder
                                    arrcredits[lngpersonid] = lngdisplayorder
                                
                                if strpersoncredits != "":
                                    strpersoncredits += ","
                                strpersoncredits += "'" + strpersoncreditcreditid + "'"
                                
                                arrpersonmoviecouples = {}
                                if intcredittype == 1:
                                    strpersoncreditcharacter = onecontent['character']
                                    strpersoncreditdepartment = ""
                                    strpersoncreditjob = ""
                                    # print(f"{strpersoncreditname}: {strpersoncreditcharacter}")
                                else:
                                    strpersoncreditcharacter = ""
                                    strpersoncreditdepartment = onecontent['department']
                                    strpersoncreditjob = onecontent['job']
                                    # print(f"{strpersoncreditname}: {strpersoncreditdepartment} {strpersoncreditjob}")
                                arrpersonmoviecouples["ID_PERSON"] = lngpersonid
                                arrpersonmoviecouples["ID_MOVIE"] = lngmovieid
                                arrpersonmoviecouples["ID_CREDIT"] = strpersoncreditcreditid
                                arrpersonmoviecouples["CAST_CHARACTER"] = strpersoncreditcharacter
                                arrpersonmoviecouples["CREW_DEPARTMENT"] = strpersoncreditdepartment
                                arrpersonmoviecouples["CREW_JOB"] = strpersoncreditjob
                                arrpersonmoviecouples["CREDIT_TYPE"] = strmoviecreditcredittype
                                arrpersonmoviecouples["DISPLAY_ORDER"] = lngcreditdisplayorder
                                # print(arrpersonmoviecouples)
                                
                                strsqltablename = "T_WC_TMDB_PERSON_MOVIE"
                                strsqlupdatecondition = f"ID_CREDIT = '{strpersoncreditcreditid}'"
                                cp.f_sqlupdatearray(strsqltablename,arrpersonmoviecouples,strsqlupdatecondition,1)
            
            if 'overview' in data:
                encoded_overview = data['overview'].replace('\n', '\\n').replace('"', '\\"')
                data['overview'] = encoded_overview
            strapimoviefordb = json.dumps(data, ensure_ascii=False)
            # strapimoviefordb = f_tmdbjsonremovekeys(strapimoviefordb,', "original_name":',', "popularity":',', "popularity":')
            # strapimoviefordb = f_tmdbjsonremovekeys(strapimoviefordb,', "name":',', "popularity":',', "popularity":')
            # strapimoviefordb = f_tmdbjsonremovekeys(strapimoviefordb,', "original_name":',', "popularity":',', "popularity":')
            #arrmoviecouples["API_RESULT"] = strapimoviefordb
            #arrmoviecouples["CRAWLER_VERSION"] = 3
            
            strsqltablename = "T_WC_TMDB_MOVIE"
            strsqlupdatecondition = f"ID_MOVIE = {lngmovieid}"
            cp.f_sqlupdatearray(strsqltablename,arrmoviecouples,strsqlupdatecondition,1)
            
            # Now delete credits that are not for this movie
            if strpersoncredits == "":
                strpersoncredits = "'0'"
            strsqldelete = "DELETE FROM T_WC_TMDB_PERSON_MOVIE WHERE ID_MOVIE = " + str(lngmovieid) + " AND ID_CREDIT NOT IN (" + strpersoncredits + ")"
            # print(f"{strsqldelete}")
            cursor2 = connectioncp.cursor()
            cursor2.execute(strsqldelete)
            connectioncp.commit()

def f_tmdbmovielangtosql(lngmovieid, strlang):
    """
    Fetch movie details in a specific language from TMDb API and store in database.

    Parameters:
    -----------
    lngmovieid : int
        The TMDb movie ID to fetch
    strlang : str
        The language code (e.g., 'fr', 'de', 'es')

    Returns:
    --------
    None
    """
    global strtmdbapidomainurl
    global headers
    global strlanguagecountry
    global strlanguage
    global strdatepattern
    global connectioncp

    if lngmovieid > 0:
        strtmdbapimovieurl = "3/movie/" + str(lngmovieid) + "?language=" + strlang
        strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapimovieurl
        # print(strtmdbapifullurl)
        data = f_tmdbfetchjson(strtmdbapifullurl, f"f_tmdbmovielangtosql({lngmovieid}, {strlang})")
        if data is None:
            return

        lngmoviestatuscode = 0
        if 'status_code' in data:
            lngmoviestatuscode = data['status_code']
        if lngmoviestatuscode <= 1:
            # API request result is not an error
            # Extract data using the keys
            strmovieoverview = ""
            if 'overview' in data:
                strmovieoverview = data['overview']
            strmovieposterpath = ""
            if 'poster_path' in data:
                strmovieposterpath = data['poster_path']
            strmovietitle = ""
            if 'title' in data:
                strmovietitle = data['title']
            strmoviebackdroppath = ""
            if 'backdrop_path' in data:
                strmoviebackdroppath = data['backdrop_path']
            strmovietagline = ""
            if 'tagline' in data:
                strmovietagline = data['tagline']
            
            if strmovietitle:
                if len(strmovietitle) > 250:
                    # If title is too long, we chop it
                    strmovietitle = strmovietitle[:250]
            
            if strmovietitle:
                if len(strmovietitle) > 250:
                    # If title is too long, we chop it
                    strmovietitle = strmovietitle[:250]
            
            arrmoviecouples = {}
            #arrmoviecouples["API_URL"] = strtmdbapimovieurl
            arrmoviecouples["OVERVIEW"] = strmovieoverview
            arrmoviecouples["ID_MOVIE"] = lngmovieid
            arrmoviecouples["LANG"] = strlang
            arrmoviecouples["POSTER_PATH"] = strmovieposterpath
            if strmovietitle != "":
                arrmoviecouples["TITLE"] = strmovietitle
            arrmoviecouples["BACKDROP_PATH"] = strmoviebackdroppath
            arrmoviecouples["TAGLINE"] = strmovietagline
            
            if 'overview' in data:
                encoded_overview = data['overview'].replace('\n', '\\n').replace('"', '\\"')
                data['overview'] = encoded_overview
            #strapimoviefordb = json.dumps(data, ensure_ascii=False)
            #arrmoviecouples["API_RESULT"] = strapimoviefordb
            #arrmoviecouples["CRAWLER_VERSION"] = 3
            
            strsqltablename = "T_WC_TMDB_MOVIE_LANG"
            strsqlupdatecondition = f"ID_MOVIE = {lngmovieid} AND LANG = '{strlang}'"
            cp.f_sqlupdatearray(strsqltablename,arrmoviecouples,strsqlupdatecondition,1)

def f_tmdbmoviekeywordstosql(lngmovieid):
    """
    Fetch and store keywords for a movie from TMDb API.

    Parameters:
    -----------
    lngmovieid : int
        The TMDb movie ID to fetch keywords for

    Returns:
    --------
    None
    """
    global strtmdbapidomainurl
    global headers

    if lngmovieid > 0:
        strtmdbapimoviekeywordsurl = "3/movie/" + str(lngmovieid) + "/keywords"
        strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapimoviekeywordsurl
        jsonmoviekeywords = f_tmdbfetchjson(strtmdbapifullurl, f"f_tmdbmoviekeywordstosql({lngmovieid})")
        if jsonmoviekeywords is None:
            return
        else:
            lngmoviekeywordsstatuscode = 0
            if 'status_code' in jsonmoviekeywords:
                lngmoviekeywordsstatuscode = jsonmoviekeywords['status_code']
            if lngmoviekeywordsstatuscode <= 1:
                # API request result is not an error
                lngkeyworddisplayorder = 0
                if jsonmoviekeywords['keywords']:
                    # Array is not empty
                    for onecontent in jsonmoviekeywords['keywords']:
                        strkeywordname = onecontent['name']
                        lngkeywordid = onecontent['id']
                        lngkeyworddisplayorder = lngkeyworddisplayorder + 1
                        arrmoviekeywordcouples = {}
                        arrmoviekeywordcouples["ID_MOVIE"] = lngmovieid
                        arrmoviekeywordcouples["ID_KEYWORD"] = lngkeywordid
                        arrmoviekeywordcouples["DISPLAY_ORDER"] = lngkeyworddisplayorder
                        # print(arrmoviekeywordcouples)
                        
                        strsqltablename = "T_WC_TMDB_MOVIE_KEYWORD"
                        strsqlupdatecondition = f"ID_MOVIE = {lngmovieid} AND ID_KEYWORD = {lngkeywordid}"
                        cp.f_sqlupdatearray(strsqltablename,arrmoviekeywordcouples,strsqlupdatecondition,1)

def f_tmdbmovieexist(lngmovieid):
    """
    Check if a movie exists in the TMDb API.

    Parameters:
    -----------
    lngmovieid : int
        The TMDb movie ID to check

    Returns:
    --------
    bool
        True if movie exists, False if not found (status_code 34)
    """
    global strtmdbapidomainurl
    global headers
    global strlanguagecountry
    global strlanguage

    # By default, we assume that this movie exists
    intresult = True
    if lngmovieid > 0:
        strtmdbapimovieurl = "3/movie/" + str(lngmovieid) + "?language=" + strlanguage
        strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapimovieurl
        # print(strtmdbapifullurl)
        data = f_tmdbfetchjson(strtmdbapifullurl, f"f_tmdbmovieexists({lngmovieid})")
        if data is None:
            return intresult
        lngmoviestatuscode = 0
        if 'status_code' in data:
            lngmoviestatuscode = data['status_code']
        if lngmoviestatuscode == 34:
            # API request result is an error:
            # The resource you requested could not be found
            intresult = False
    return intresult

def f_tmdbmoviedelete(lngmovieid):
    """
    Delete a movie and all related records from the database.

    Parameters:
    -----------
    lngmovieid : int
        The TMDb movie ID to delete

    Returns:
    --------
    None
    """
    global connectioncp

    if lngmovieid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_MOVIE"
        strsqlupdatecondition = f"ID_MOVIE = {lngmovieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
        strsqltablename = "T_WC_TMDB_MOVIE_LANG"
        strsqlupdatecondition = f"ID_MOVIE = {lngmovieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
        strsqltablename = "T_WC_TMDB_MOVIE_LIST"
        strsqlupdatecondition = f"ID_MOVIE = {lngmovieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
        strsqltablename = "T_WC_TMDB_MOVIE_GENRE"
        strsqlupdatecondition = f"ID_MOVIE = {lngmovieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
        strsqltablename = "T_WC_TMDB_MOVIE_KEYWORD"
        strsqlupdatecondition = f"ID_MOVIE = {lngmovieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
        strsqltablename = "T_WC_TMDB_MOVIE_LEMME"
        strsqlupdatecondition = f"ID_MOVIE = {lngmovieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
        strsqltablename = "T_WC_TMDB_MOVIE_IMAGE"
        strsqlupdatecondition = f"ID_MOVIE = {lngmovieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
        strsqltablename = "T_WC_TMDB_MOVIE_VIDEO"
        strsqlupdatecondition = f"ID_MOVIE = {lngmovieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
        strsqltablename = "T_WC_TMDB_PERSON_MOVIE"
        strsqlupdatecondition = f"ID_MOVIE = {lngmovieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdbmoviesetcreditscompleted(lngmovieid):
    """
    Mark a movie's credits as fully processed by setting TIM_CREDITS_COMPLETED timestamp.

    Parameters:
    -----------
    lngmovieid : int
        The TMDb movie ID to update

    Returns:
    --------
    None
    """
    global paris_tz
    global connectioncp

    if lngmovieid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_MOVIE"
        strsqlupdatecondition = f"ID_MOVIE = {lngmovieid}"
        strtimcreditscompleted = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        strsqlupdatesetclause = f"TIM_CREDITS_COMPLETED = '{strtimcreditscompleted}', TIM_UPDATED = '{strtimcreditscompleted}'"
        strsqlupdate = f"UPDATE {strsqltablename} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdbmoviesetkeywordscompleted(lngmovieid):
    """
    Mark a movie's keywords as fully processed by setting TIM_KEYWORDS_COMPLETED timestamp.

    Parameters:
    -----------
    lngmovieid : int
        The TMDb movie ID to update

    Returns:
    --------
    None
    """
    global paris_tz
    global connectioncp

    if lngmovieid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_MOVIE"
        strsqlupdatecondition = f"ID_MOVIE = {lngmovieid}"
        strtimkeywordscompleted = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        strsqlupdatesetclause = f"TIM_KEYWORDS_COMPLETED = '{strtimkeywordscompleted}'"
        strsqlupdate = f"UPDATE {strsqltablename} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdbmoviesetwikidatacompleted(lngmovieid):
    """
    Mark a movie's Wikidata integration as completed by setting TIM_WIKIDATA_COMPLETED timestamp.

    Parameters:
    -----------
    lngmovieid : int
        The TMDb movie ID to update

    Returns:
    --------
    None
    """
    global paris_tz
    global connectioncp

    if lngmovieid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_MOVIE"
        strsqlupdatecondition = f"ID_MOVIE = {lngmovieid}"
        strtimwikidatacompleted = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        strsqlupdatesetclause = f"TIM_WIKIDATA_COMPLETED = '{strtimwikidatacompleted}'"
        strsqlupdate = f"UPDATE {strsqltablename} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdbmoviesetwikipediacompleted(lngmovieid):
    """
    Mark a movie's Wikipedia integration as completed by setting TIM_WIKIPEDIA_COMPLETED timestamp.

    Parameters:
    -----------
    lngmovieid : int
        The TMDb movie ID to update

    Returns:
    --------
    None
    """
    global paris_tz
    global connectioncp

    if lngmovieid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_MOVIE"
        strsqlupdatecondition = f"ID_MOVIE = {lngmovieid}"
        strtimwikidatacompleted = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        strsqlupdatesetclause = f"TIM_WIKIPEDIA_COMPLETED = '{strtimwikidatacompleted}'"
        strsqlupdate = f"UPDATE {strsqltablename} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdbmovieimagestosql(lngmovieid):
    """
    Fetch and store images for a movie from TMDb API.

    Parameters:
    -----------
    lngmovieid : int
        The TMDb movie ID to fetch images for

    Returns:
    --------
    bool
        True if successful, False if failed
    """
    f_tmdbcontentimagesstosql(lngmovieid, "movie", "T_WC_TMDB_MOVIE", "T_WC_TMDB_MOVIE_IMAGE", "ID_MOVIE", "POSTER_PATH", "poster", "T_WC_TMDB_MOVIE_LANG")

def f_tmdbmovievideotosql(lngmovieid, strlang):
    """
    Fetch and store videos for a movie in a specific language from TMDb API.

    Parameters:
    -----------
    lngmovieid : int
        The TMDb movie ID to fetch videos for
    strlang : str
        The language code (e.g., 'en', 'fr')

    Returns:
    --------
    bool
        True if successful, False if failed
    """
    f_tmdbcontentvideosstosql(lngmovieid, "movie", "T_WC_TMDB_MOVIE", "T_WC_TMDB_MOVIE_VIDEO", "ID_MOVIE", strlang)

def f_tmdbmovietosqleverything(lngmovieid):
    """
    Fetch and store complete movie data including details, credits, keywords, images, and videos.

    Parameters:
    -----------
    lngmovieid : int
        The TMDb movie ID to fetch

    Returns:
    --------
    None
    """
    f_tmdbmovietosql(lngmovieid)
    f_tmdbmovielangtosql(lngmovieid,'fr')
    f_tmdbmoviesetcreditscompleted(lngmovieid)
    f_tmdbmoviekeywordstosql(lngmovieid)
    f_tmdbmoviesetkeywordscompleted(lngmovieid)
    f_tmdbmovieimagestosql(lngmovieid)
    f_tmdbmovievideotosql(lngmovieid,'en')
    f_tmdbmovievideotosql(lngmovieid,'fr')

# https://developer.themoviedb.org/reference/tv-series-details

def f_tmdbserietosql(lngserieid):
    """
    Fetch TV series details from TMDb API and store in database.

    Parameters:
    -----------
    lngserieid : int
        The TMDb TV series ID to fetch

    Returns:
    --------
    bool
        True if successful, False if failed or invalid ID
    """
    global strtmdbapidomainurl
    global headers
    global strlanguagecountry
    global strlanguage
    global strdatepattern
    global connectioncp
    global strsqlns

    if lngserieid > 0:
        # New TMDb API call with append_to_response
        strtmdbapiserieurl = "3/tv/" + str(lngserieid) + "?append_to_response=credits,alternative_titles,external_ids&language=" + strlanguage
        strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapiserieurl
        #print(strtmdbapifullurl)
        data = f_tmdbfetchjson(strtmdbapifullurl, f"f_tmdbserietosql({lngserieid})")
        if data is None:
            return

        # Parse the JSON data into a dictionary
        
        lngseriestatuscode = 0
        if 'status_code' in data:
            lngseriestatuscode = data['status_code']
        if lngseriestatuscode <= 1:
            # API request result is not an error
            # Extract data using the keys
            strserieidimdb = ""
            if 'external_ids' in data:
                if 'imdb_id' in data['external_ids']:
                    strserieidimdb = data['external_ids']['imdb_id']
            
            strserieoverview = ""
            if 'overview' in data:
                strserieoverview = data['overview']
            
            # Process first air date with proper validation
            strseriefirstairdate = ""
            lngseriefirstairyear = None
            lngseriefirstairmonth = None
            lngseriefirstairday = None
            if 'first_air_date' in data:
                strseriefirstairdate = data['first_air_date']
                if strseriefirstairdate and re.match(strdatepattern, strseriefirstairdate):
                    lngseriefirstairyear, lngseriefirstairmonth, lngseriefirstairday = map(int, strseriefirstairdate.split('-'))
                else:
                    strseriefirstairdate = None
            
            # Process last air date with proper validation
            strserielastairdate = ""
            lngserielastairyear = None
            lngserielastairmonth = None
            lngserielastairday = None
            if 'last_air_date' in data:
                strserielastairdate = data['last_air_date']
                if strserielastairdate and re.match(strdatepattern, strserielastairdate):
                    lngserielastairyear, lngserielastairmonth, lngserielastairday = map(int, strserielastairdate.split('-'))
                else:
                    strserielastairdate = None
            
            strserieposterpath = ""
            if 'poster_path' in data:
                strserieposterpath = data['poster_path']
            
            strseriehomepage = ""
            if 'homepage' in data:
                strseriehomepage = data['homepage']
            
            strserietitle = ""
            if 'name' in data:
                strserietitle = data['name']
            
            strserieoriginallanguage = ""
            if 'original_language' in data:
                strserieoriginallanguage = data['original_language']
            
            dblseriepopularity = 0
            if 'popularity' in data:
                dblseriepopularity = data['popularity']
            
            strseriebackdroppath = ""
            if 'backdrop_path' in data:
                strseriebackdroppath = data['backdrop_path']
            
            intserieadult = 0
            if 'adult' in data:
                booserieadult = data['adult']
                if booserieadult:
                    intserieadult = 1
            
            strserieoriginaltitle = ""
            if 'original_name' in data:
                strserieoriginaltitle = data['original_name']
            
            strseriestatus = ""
            if 'status' in data:
                strseriestatus = data['status']
            
            strseriegenres = ""
            if 'genres' in data:
                arrseriegenres = data['genres']
                strseriegenres = "|"
                arrgenrenames = [genre['name'] for genre in arrseriegenres]
                for strvalue in arrgenrenames:
                    strseriegenres += strvalue + "|"
            
            strserietagline = ""
            if 'tagline' in data:
                strserietagline = data['tagline']
            
            dblserievoteaverage = 0
            if 'vote_average' in data:
                dblserievoteaverage = data['vote_average']
            
            lngserievotecount = 0
            if 'vote_count' in data:
                lngserievotecount = data['vote_count']
            
            strserieidwikidata = ""
            if 'external_ids' in data:
                if 'wikidata_id' in data['external_ids']:
                    strserieidwikidata = data['external_ids']['wikidata_id']

            lngserieidtvdb = None
            if 'external_ids' in data:
                if data['external_ids'].get('tvdb_id'):
                    lngserieidtvdb = data['external_ids']['tvdb_id']

            # Add TV-specific fields
            lngnumberofepisodes = 0
            if 'number_of_episodes' in data:
                lngnumberofepisodes = data['number_of_episodes']
            
            lngnumberofseasons = 0
            if 'number_of_seasons' in data:
                lngnumberofseasons = data['number_of_seasons']
            
            strserietype = ""
            if 'type' in data:
                strserietype = data['type']

            # Activity signals (drive selective season/episode refresh)
            intinproduction = None
            if 'in_production' in data:
                booinproduction = data['in_production']
                if booinproduction is True:
                    intinproduction = 1
                elif booinproduction is False:
                    intinproduction = 0

            strnextepisodedatair = None
            lngnextepisodeseasonnumber = None
            lngnextepisodenumber = None
            if 'next_episode_to_air' in data and data['next_episode_to_air']:
                arrnextepisode = data['next_episode_to_air']
                if 'air_date' in arrnextepisode and arrnextepisode['air_date']:
                    if re.match(strdatepattern, arrnextepisode['air_date']):
                        strnextepisodedatair = arrnextepisode['air_date']
                if 'season_number' in arrnextepisode:
                    lngnextepisodeseasonnumber = arrnextepisode['season_number']
                if 'episode_number' in arrnextepisode:
                    lngnextepisodenumber = arrnextepisode['episode_number']

            strlastepisodedatair = None
            lnglastepisodeseasonnumber = None
            lnglastepisodenumber = None
            if 'last_episode_to_air' in data and data['last_episode_to_air']:
                arrlastepisode = data['last_episode_to_air']
                if 'air_date' in arrlastepisode and arrlastepisode['air_date']:
                    if re.match(strdatepattern, arrlastepisode['air_date']):
                        strlastepisodedatair = arrlastepisode['air_date']
                if 'season_number' in arrlastepisode:
                    lnglastepisodeseasonnumber = arrlastepisode['season_number']
                if 'episode_number' in arrlastepisode:
                    lnglastepisodenumber = arrlastepisode['episode_number']

            # Process production countries
            strseriecountries = ""
            strcountryidlist = ""
            lngcountrydisplayorder = 0
            if 'production_countries' in data:
                arrseriecountries = data['production_countries']
                strseriecountries = "|"
                arrcountrynames = [country['iso_3166_1'] for country in arrseriecountries]
                for strvalue in arrcountrynames:
                    strseriecountries += strvalue + "|"
                    lngcountrydisplayorder = lngcountrydisplayorder + 1
                    if strcountryidlist != "":
                        strcountryidlist += ","
                    strcountryidlist += "'" + strvalue + "'"
                    arrseriecountrycouples = {}
                    arrseriecountrycouples["ID_SERIE"] = lngserieid
                    arrseriecountrycouples["COUNTRY_CODE"] = strvalue
                    arrseriecountrycouples["DISPLAY_ORDER"] = lngcountrydisplayorder
                    
                    strsqltablename = "T_WC_TMDB_SERIE_PRODUCTION_COUNTRY"
                    strsqlupdatecondition = f"ID_SERIE = {lngserieid} AND COUNTRY_CODE = '{strvalue}'"
                    cp.f_sqlupdatearray(strsqltablename, arrseriecountrycouples, strsqlupdatecondition, 1)
            
            if strcountryidlist != "":
                strsqldelete = "DELETE FROM " + strsqlns + "TMDB_SERIE_PRODUCTION_COUNTRY WHERE ID_SERIE = " + str(lngserieid) + " AND COUNTRY_CODE NOT IN (" + strcountryidlist + ") "
                cursor2 = connectioncp.cursor()
                cursor2.execute(strsqldelete)
                connectioncp.commit()
            
            # Process spoken languages
            strseriespokenlanguages = ""
            strspokenlanguageidlist = ""
            lngspokenlanguagedisplayorder = 0
            if 'spoken_languages' in data:
                arrseriespokenlanguages = data['spoken_languages']
                strseriespokenlanguages = "|"
                arrspokenlanguagenames = [spokenlanguage['iso_639_1'] for spokenlanguage in arrseriespokenlanguages]
                for strvalue in arrspokenlanguagenames:
                    strseriespokenlanguages += strvalue + "|"
                    lngspokenlanguagedisplayorder = lngspokenlanguagedisplayorder + 1
                    if strspokenlanguageidlist != "":
                        strspokenlanguageidlist += ","
                    strspokenlanguageidlist += "'" + strvalue + "'"
                    arrseriespokenlanguagecouples = {}
                    arrseriespokenlanguagecouples["ID_SERIE"] = lngserieid
                    arrseriespokenlanguagecouples["SPOKEN_LANGUAGE"] = strvalue
                    arrseriespokenlanguagecouples["DISPLAY_ORDER"] = lngspokenlanguagedisplayorder
                    
                    strsqltablename = "T_WC_TMDB_SERIE_SPOKEN_LANGUAGE"
                    strsqlupdatecondition = f"ID_SERIE = {lngserieid} AND SPOKEN_LANGUAGE = '{strvalue}'"
                    cp.f_sqlupdatearray(strsqltablename, arrseriespokenlanguagecouples, strsqlupdatecondition, 1)
            
            if strspokenlanguageidlist != "":
                strsqldelete = "DELETE FROM " + strsqlns + "TMDB_SERIE_SPOKEN_LANGUAGE WHERE ID_SERIE = " + str(lngserieid) + " AND SPOKEN_LANGUAGE NOT IN (" + strspokenlanguageidlist + ") "
                cursor2 = connectioncp.cursor()
                cursor2.execute(strsqldelete)
                connectioncp.commit()
            
            # Process networks (TV-specific)
            strnetworkidlist = ""
            lngnetworkdisplayorder = 0
            if 'networks' in data and data['networks']:
                for onecontent in data['networks']:
                    lngnetworkid = onecontent['id']
                    lngnetworkdisplayorder = lngnetworkdisplayorder + 1
                    if strnetworkidlist != "":
                        strnetworkidlist += ","
                    strnetworkidlist += str(lngnetworkid)
                    arrserieneworkcouples = {}
                    arrserieneworkcouples["ID_SERIE"] = lngserieid
                    arrserieneworkcouples["ID_NETWORK"] = lngnetworkid
                    arrserieneworkcouples["DISPLAY_ORDER"] = lngnetworkdisplayorder
                    
                    strsqltablename = "T_WC_TMDB_SERIE_NETWORK"
                    strsqlupdatecondition = f"ID_SERIE = {lngserieid} AND ID_NETWORK = {lngnetworkid}"
                    cp.f_sqlupdatearray(strsqltablename, arrserieneworkcouples, strsqlupdatecondition, 1)
            
            if strnetworkidlist != "":
                strsqldelete = "DELETE FROM " + strsqlns + "TMDB_SERIE_NETWORK WHERE ID_SERIE = " + str(lngserieid) + " AND ID_NETWORK NOT IN (" + strnetworkidlist + ") "
                cursor2 = connectioncp.cursor()
                cursor2.execute(strsqldelete)
                connectioncp.commit()
            
            # Process production companies
            strcompanyidlist = ""
            lngcompanydisplayorder = 0
            if 'production_companies' in data and data['production_companies']:
                for onecontent in data['production_companies']:
                    lngcompanyid = onecontent['id']
                    lngcompanydisplayorder = lngcompanydisplayorder + 1
                    if strcompanyidlist != "":
                        strcompanyidlist += ","
                    strcompanyidlist += str(lngcompanyid)
                    arrseriecompanycouples = {}
                    arrseriecompanycouples["ID_SERIE"] = lngserieid
                    arrseriecompanycouples["ID_COMPANY"] = lngcompanyid
                    arrseriecompanycouples["DISPLAY_ORDER"] = lngcompanydisplayorder
                    
                    strsqltablename = "T_WC_TMDB_SERIE_COMPANY"
                    strsqlupdatecondition = f"ID_SERIE = {lngserieid} AND ID_COMPANY = {lngcompanyid}"
                    cp.f_sqlupdatearray(strsqltablename, arrseriecompanycouples, strsqlupdatecondition, 1)
            
            if strcompanyidlist != "":
                strsqldelete = "DELETE FROM " + strsqlns + "TMDB_SERIE_COMPANY WHERE ID_SERIE = " + str(lngserieid) + " AND ID_COMPANY NOT IN (" + strcompanyidlist + ") "
                cursor2 = connectioncp.cursor()
                cursor2.execute(strsqldelete)
                connectioncp.commit()
            
            # Process genres
            strgenreidlist = ""
            lnggenredisplayorder = 0
            if 'genres' in data and data['genres']:
                for onecontent in data['genres']:
                    lnggenreid = onecontent['id']
                    lnggenredisplayorder = lnggenredisplayorder + 1
                    if strgenreidlist != "":
                        strgenreidlist += ","
                    strgenreidlist += str(lnggenreid)
                    arrseriegenrecouples = {}
                    arrseriegenrecouples["ID_SERIE"] = lngserieid
                    arrseriegenrecouples["ID_GENRE"] = lnggenreid
                    arrseriegenrecouples["DISPLAY_ORDER"] = lnggenredisplayorder
                    
                    strsqltablename = "T_WC_TMDB_SERIE_GENRE"
                    strsqlupdatecondition = f"ID_SERIE = {lngserieid} AND ID_GENRE = {lnggenreid}"
                    cp.f_sqlupdatearray(strsqltablename, arrseriegenrecouples, strsqlupdatecondition, 1)
            
            if strgenreidlist != "":
                strsqldelete = "DELETE FROM " + strsqlns + "TMDB_SERIE_GENRE WHERE ID_SERIE = " + str(lngserieid) + " AND ID_GENRE NOT IN (" + strgenreidlist + ") "
                cursor2 = connectioncp.cursor()
                cursor2.execute(strsqldelete)
                connectioncp.commit()
            
            # Field length validation
            if strserietitle:
                if len(strserietitle) > 250:
                    strserietitle = strserietitle[:250]
            
            if strserieoriginaltitle:
                if len(strserieoriginaltitle) > 250:
                    strserieoriginaltitle = strserieoriginaltitle[:250]
            
            if strseriehomepage:
                if len(strseriehomepage) > 500:
                    strseriehomepage = strseriehomepage[:500]
            
            # Prepare main record data
            arrseriecouples = {}
            #arrseriecouples["API_URL"] = strtmdbapiserieurl
            arrseriecouples["ID_SERIE"] = lngserieid
            
            if strserieidimdb:
                arrseriecouples["ID_IMDB"] = strserieidimdb
            else:
                arrseriecouples["ID_IMDB"] = ""
            
            if strserieidwikidata:
                arrseriecouples["ID_WIKIDATA"] = strserieidwikidata
            else:
                arrseriecouples["ID_WIKIDATA"] = ""

            # ID_TVDB is an int column (mirrors SEASON/EPISODE); only set when present
            if lngserieidtvdb:
                arrseriecouples["ID_TVDB"] = lngserieidtvdb

            arrseriecouples["OVERVIEW"] = strserieoverview
            
            # Date fields
            if strseriefirstairdate:
                arrseriecouples["DAT_FIRST_AIR"] = strseriefirstairdate
            arrseriecouples["FIRST_AIR_YEAR"] = lngseriefirstairyear
            arrseriecouples["FIRST_AIR_MONTH"] = lngseriefirstairmonth
            arrseriecouples["FIRST_AIR_DAY"] = lngseriefirstairday
            
            if strserielastairdate:
                arrseriecouples["DAT_LAST_AIR"] = strserielastairdate
            arrseriecouples["LAST_AIR_YEAR"] = lngserielastairyear
            arrseriecouples["LAST_AIR_MONTH"] = lngserielastairmonth
            arrseriecouples["LAST_AIR_DAY"] = lngserielastairday
            
            arrseriecouples["POSTER_PATH"] = strserieposterpath
            arrseriecouples["HOMEPAGE_URL"] = strseriehomepage
            
            if strserietitle != "":
                arrseriecouples["TITLE"] = strserietitle
            else:
                arrseriecouples["TITLE"] = ""
            
            arrseriecouples["ORIGINAL_LANGUAGE"] = strserieoriginallanguage
            arrseriecouples["POPULARITY"] = dblseriepopularity
            arrseriecouples["BACKDROP_PATH"] = strseriebackdroppath
            arrseriecouples["ORIGINAL_TITLE"] = strserieoriginaltitle
            arrseriecouples["STATUS"] = strseriestatus
            arrseriecouples["GENRES"] = strseriegenres
            arrseriecouples["ADULT"] = intserieadult
            arrseriecouples["TAGLINE"] = strserietagline
            arrseriecouples["VOTE_AVERAGE"] = dblserievoteaverage
            arrseriecouples["VOTE_COUNT"] = lngserievotecount
            arrseriecouples["COUNTRIES"] = strseriecountries
            arrseriecouples["SPOKEN_LANGUAGES"] = strseriespokenlanguages
            
            # TV-specific fields
            arrseriecouples["NUMBER_OF_EPISODES"] = lngnumberofepisodes
            arrseriecouples["NUMBER_OF_SEASONS"] = lngnumberofseasons
            arrseriecouples["SERIE_TYPE"] = strserietype

            # Activity signals (always set so they get cleared when TMDb drops them)
            arrseriecouples["IN_PRODUCTION"] = intinproduction
            arrseriecouples["NEXT_EPISODE_DAT_AIR"] = strnextepisodedatair
            arrseriecouples["NEXT_EPISODE_SEASON_NUMBER"] = lngnextepisodeseasonnumber
            arrseriecouples["NEXT_EPISODE_NUMBER"] = lngnextepisodenumber
            arrseriecouples["LAST_EPISODE_DAT_AIR"] = strlastepisodedatair
            arrseriecouples["LAST_EPISODE_SEASON_NUMBER"] = lnglastepisodeseasonnumber
            arrseriecouples["LAST_EPISODE_NUMBER"] = lnglastepisodenumber
            
            # Store created_by array for later use with credits
            arrcreatedby = []
            if 'created_by' in data and data['created_by']:
                arrcreatedby = data['created_by']  # Store the created_by array for later use with credits
            
            # Process credits
            strpersoncredits = ""
            lngcreditdisplayorder = 0
            arrcredittype = {1: 'cast', 2: 'crew'}
            for intcredittype, strseriecreditcredittype in arrcredittype.items():
                if intcredittype == 1:
                    strtitle = "Cast"
                else:
                    strtitle = "Crew"
                
                if 'credits' in data:
                    if strseriecreditcredittype in data['credits']:
                        arrseriecredits = data['credits'][strseriecreditcredittype]
                        lngdisplayorder = 0
                        arrcredits = {}
                        if arrseriecredits:
                            for onecontent in arrseriecredits:
                                lngdisplayorder += 1
                                strpersoncreditname = onecontent['name']
                                strpersoncreditcreditid = onecontent['credit_id']
                                lngpersonid = onecontent['id']
                                if lngpersonid in arrcredits:
                                    lngcreditdisplayorder = arrcredits[lngpersonid]
                                else:
                                    lngcreditdisplayorder = lngdisplayorder
                                    arrcredits[lngpersonid] = lngdisplayorder
                                
                                if strpersoncredits != "":
                                    strpersoncredits += ","
                                strpersoncredits += "'" + strpersoncreditcreditid + "'"
                                
                                arrpersonseriecouples = {}
                                if intcredittype == 1:
                                    strpersoncreditcharacter = onecontent['character']
                                    strpersoncreditdepartment = ""
                                    strpersoncreditjob = ""
                                else:
                                    strpersoncreditcharacter = ""
                                    strpersoncreditdepartment = onecontent['department']
                                    strpersoncreditjob = onecontent['job']
                                    
                                    # Check if this person is in the created_by array
                                    is_creator = False
                                    for creator in arrcreatedby:
                                        if creator['id'] == lngpersonid:
                                            is_creator = True
                                            strcreatorcreditid = creator['credit_id']
                                            break
                                    
                                    # If person is a creator, add a special record for them
                                    if is_creator:
                                        # Generate a unique credit ID for the creator entry
                                        if strpersoncredits != "":
                                            strpersoncredits += ","
                                        strpersoncredits += "'" + strcreatorcreditid + "'"
                                        
                                        arrcreatorcouples = {}
                                        arrcreatorcouples["ID_PERSON"] = lngpersonid
                                        arrcreatorcouples["ID_SERIE"] = lngserieid
                                        arrcreatorcouples["ID_CREDIT"] = strcreatorcreditid
                                        arrcreatorcouples["CAST_CHARACTER"] = ""
                                        arrcreatorcouples["CREW_DEPARTMENT"] = "Creator"
                                        arrcreatorcouples["CREW_JOB"] = "Creator"
                                        arrcreatorcouples["CREDIT_TYPE"] = "crew"
                                        arrcreatorcouples["DISPLAY_ORDER"] = lngcreditdisplayorder
                                        
                                        strsqltablename = "T_WC_TMDB_PERSON_SERIE"
                                        strsqlupdatecondition = f"ID_CREDIT = '{strcreatorcreditid}'"
                                        cp.f_sqlupdatearray(strsqltablename, arrcreatorcouples, strsqlupdatecondition, 1)
                                        lngcreditdisplayorder += 1
                                
                                arrpersonseriecouples["ID_PERSON"] = lngpersonid
                                arrpersonseriecouples["ID_SERIE"] = lngserieid
                                arrpersonseriecouples["ID_CREDIT"] = strpersoncreditcreditid
                                arrpersonseriecouples["CAST_CHARACTER"] = strpersoncreditcharacter
                                arrpersonseriecouples["CREW_DEPARTMENT"] = strpersoncreditdepartment
                                arrpersonseriecouples["CREW_JOB"] = strpersoncreditjob
                                arrpersonseriecouples["CREDIT_TYPE"] = strseriecreditcredittype
                                arrpersonseriecouples["DISPLAY_ORDER"] = lngcreditdisplayorder
                                
                                strsqltablename = "T_WC_TMDB_PERSON_SERIE"
                                strsqlupdatecondition = f"ID_CREDIT = '{strpersoncreditcreditid}'"
                                cp.f_sqlupdatearray(strsqltablename, arrpersonseriecouples, strsqlupdatecondition, 1)
            
            # Encode and store API result
            if 'overview' in data:
                encoded_overview = data['overview'].replace('\n', '\\n').replace('"', '\\"')
                data['overview'] = encoded_overview
            
            #strapiseriefordb = json.dumps(data, ensure_ascii=False)
            #arrseriecouples["API_RESULT"] = strapiseriefordb
            #arrseriecouples["CRAWLER_VERSION"] = 3
            
            # Update main serie record
            strsqltablename = "T_WC_TMDB_SERIE"
            strsqlupdatecondition = f"ID_SERIE = {lngserieid}"
            cp.f_sqlupdatearray(strsqltablename, arrseriecouples, strsqlupdatecondition, 1)
            
            # Process any creators that weren't found in the crew credits
            if arrcreatedby:
                # Keep track of which creators have been processed
                processed_creator_ids = set()
                
                # Check which creators were already processed during credit processing
                if 'credits' in data:
                    for intcredittype, strseriecreditcredittype in arrcredittype.items():
                        if strseriecreditcredittype in data['credits']:
                            for onecontent in data['credits'][strseriecreditcredittype]:
                                lngpersonid = onecontent['id']
                                for creator in arrcreatedby:
                                    if creator['id'] == lngpersonid:
                                        processed_creator_ids.add(lngpersonid)
                
                # Add any creators that weren't processed
                for creator in arrcreatedby:
                    lngpersonid = creator['id']
                    if lngpersonid not in processed_creator_ids:
                        lngcreditdisplayorder += 1
                        strcreatorcreditid = creator['credit_id']
                        
                        if strpersoncredits != "":
                            strpersoncredits += ","
                        strpersoncredits += "'" + strcreatorcreditid + "'"
                        
                        arrcreatorcouples = {}
                        arrcreatorcouples["ID_PERSON"] = lngpersonid
                        arrcreatorcouples["ID_SERIE"] = lngserieid
                        arrcreatorcouples["ID_CREDIT"] = strcreatorcreditid
                        arrcreatorcouples["CAST_CHARACTER"] = ""
                        arrcreatorcouples["CREW_DEPARTMENT"] = "Creator"
                        arrcreatorcouples["CREW_JOB"] = "Creator"
                        arrcreatorcouples["CREDIT_TYPE"] = "crew"
                        arrcreatorcouples["DISPLAY_ORDER"] = lngcreditdisplayorder
                        
                        strsqltablename = "T_WC_TMDB_PERSON_SERIE"
                        strsqlupdatecondition = f"ID_CREDIT = '{strcreatorcreditid}'"
                        cp.f_sqlupdatearray(strsqltablename, arrcreatorcouples, strsqlupdatecondition, 1)
                        
            
            # Clean up obsolete credits
            if strpersoncredits == "":
                strpersoncredits = "'0'"
            strsqldelete = "DELETE FROM T_WC_TMDB_PERSON_SERIE WHERE ID_SERIE = " + str(lngserieid) + " AND ID_CREDIT NOT IN (" + strpersoncredits + ")"
            cursor2 = connectioncp.cursor()
            cursor2.execute(strsqldelete)
            connectioncp.commit()

def f_tmdbserielangtosql(lngserieid, strlang):
    """
    Fetch TV series details in a specific language from TMDb API and store in database.

    Parameters:
    -----------
    lngserieid : int
        The TMDb TV series ID to fetch
    strlang : str
        The language code (e.g., 'fr', 'de', 'es')

    Returns:
    --------
    None
    """
    global strtmdbapidomainurl
    global headers
    global strlanguagecountry
    global strlanguage
    global strdatepattern
    global connectioncp

    if lngserieid > 0:
        # New TMDb API call with append_to_response since 2024-05-24 10:00
        strtmdbapiserieurl = "3/tv/" + str(lngserieid) + "?language=" + strlang
        strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapiserieurl
        # print(strtmdbapifullurl)
        data = f_tmdbfetchjson(strtmdbapifullurl, f"f_tmdbserielangtosql({lngserieid}, {strlang})")
        if data is None:
            return
        lngseriestatuscode = 0
        if 'status_code' in data:
            lngseriestatuscode = data['status_code']
        if lngseriestatuscode <= 1:
            # API request result is not an error
            # Extract data using the keys
            strserieoverview = ""
            if 'overview' in data:
                strserieoverview = data['overview']
            strserieposterpath = ""
            if 'poster_path' in data:
                strserieposterpath = data['poster_path']
            strserietitle = ""
            if 'name' in data:
                strserietitle = data['name']
            strseriebackdroppath = ""
            if 'backdrop_path' in data:
                strseriebackdroppath = data['backdrop_path']
            strserietagline = ""
            if 'tagline' in data:
                strserietagline = data['tagline']
            
            if strserietitle:
                if len(strserietitle) > 250:
                    # If title is too long, we chop it
                    strserietitle = strserietitle[:250]
            
            if strserietitle:
                if len(strserietitle) > 250:
                    # If title is too long, we chop it
                    strserietitle = strserietitle[:250]
            
            arrseriecouples = {}
            #arrseriecouples["API_URL"] = strtmdbapiserieurl
            arrseriecouples["OVERVIEW"] = strserieoverview
            arrseriecouples["ID_SERIE"] = lngserieid
            arrseriecouples["LANG"] = strlang
            arrseriecouples["POSTER_PATH"] = strserieposterpath
            if strserietitle != "":
                arrseriecouples["TITLE"] = strserietitle
            arrseriecouples["BACKDROP_PATH"] = strseriebackdroppath
            arrseriecouples["TAGLINE"] = strserietagline
            
            if 'overview' in data:
                encoded_overview = data['overview'].replace('\n', '\\n').replace('"', '\\"')
                data['overview'] = encoded_overview
            #strapiseriefordb = json.dumps(data, ensure_ascii=False)
            #arrseriecouples["API_RESULT"] = strapiseriefordb
            #arrseriecouples["CRAWLER_VERSION"] = 3
            
            strsqltablename = "T_WC_TMDB_SERIE_LANG"
            strsqlupdatecondition = f"ID_SERIE = {lngserieid} AND LANG = '{strlang}'"
            cp.f_sqlupdatearray(strsqltablename,arrseriecouples,strsqlupdatecondition,1)

def f_tmdbseriekeywordstosql(lngserieid):
    """
    Fetch and store keywords for a TV series from TMDb API.

    Parameters:
    -----------
    lngserieid : int
        The TMDb TV series ID to fetch keywords for

    Returns:
    --------
    None
    """
    global strtmdbapidomainurl
    global headers

    if lngserieid > 0:
        strtmdbapiseriekeywordsurl = "3/tv/" + str(lngserieid) + "/keywords"
        strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapiseriekeywordsurl
        jsonseriekeywords = f_tmdbfetchjson(strtmdbapifullurl, f"f_tmdbseriekeywordstosql({lngserieid})")
        if jsonseriekeywords is None:
            return
        else:
            lngseriekeywordsstatuscode = 0
            if 'status_code' in jsonseriekeywords:
                lngseriekeywordsstatuscode = jsonseriekeywords['status_code']
            if lngseriekeywordsstatuscode <= 1:
                # API request result is not an error
                lngkeyworddisplayorder = 0
                if 'results' in jsonseriekeywords:
                    if jsonseriekeywords['results']:
                        # Array is not empty
                        for onecontent in jsonseriekeywords['results']:
                            strkeywordname = onecontent['name']
                            lngkeywordid = onecontent['id']
                            lngkeyworddisplayorder = lngkeyworddisplayorder + 1
                            arrseriekeywordcouples = {}
                            arrseriekeywordcouples["ID_SERIE"] = lngserieid
                            arrseriekeywordcouples["ID_KEYWORD"] = lngkeywordid
                            arrseriekeywordcouples["DISPLAY_ORDER"] = lngkeyworddisplayorder
                            # print(arrseriekeywordcouples)
                            
                            strsqltablename = "T_WC_TMDB_SERIE_KEYWORD"
                            strsqlupdatecondition = f"ID_SERIE = {lngserieid} AND ID_KEYWORD = {lngkeywordid}"
                            cp.f_sqlupdatearray(strsqltablename,arrseriekeywordcouples,strsqlupdatecondition,1)

def f_tmdbserieexist(lngserieid):
    """
    Check if a TV series exists in the TMDb API.

    Parameters:
    -----------
    lngserieid : int
        The TMDb TV series ID to check

    Returns:
    --------
    bool
        True if series exists, False if not found (status_code 34)
    """
    global strtmdbapidomainurl
    global headers
    global strlanguagecountry
    global strlanguage

    # By default, we assume that this serie exists
    intresult = True
    if lngserieid > 0:
        strtmdbapiserieurl = "3/tv/" + str(lngserieid) + "?language=" + strlanguage
        strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapiserieurl
        # print(strtmdbapifullurl)
        data = f_tmdbfetchjson(strtmdbapifullurl, f"f_tmdbserieexist({lngserieid})")
        if data is None:
            return intresult
        lngseriestatuscode = 0
        if 'status_code' in data:
            lngseriestatuscode = data['status_code']
        if lngseriestatuscode == 34:
            # API request result is an error:
            # The resource you requested could not be found
            intresult = False
    return intresult

def f_tmdbseriedelete(lngserieid):
    """
    Delete a TV series and all related records from the database.

    Parameters:
    -----------
    lngserieid : int
        The TMDb TV series ID to delete

    Returns:
    --------
    None
    """
    global connectioncp

    if lngserieid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_SERIE"
        strsqlupdatecondition = f"ID_SERIE = {lngserieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
        strsqltablename = "T_WC_TMDB_SERIE_LANG"
        strsqlupdatecondition = f"ID_SERIE = {lngserieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
        strsqltablename = "T_WC_TMDB_SERIE_LIST"
        strsqlupdatecondition = f"ID_SERIE = {lngserieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
        strsqltablename = "T_WC_TMDB_SERIE_GENRE"
        strsqlupdatecondition = f"ID_SERIE = {lngserieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
        strsqltablename = "T_WC_TMDB_SERIE_KEYWORD"
        strsqlupdatecondition = f"ID_SERIE = {lngserieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        """
        strsqltablename = "T_WC_TMDB_SERIE_LEMME"
        strsqlupdatecondition = f"ID_SERIE = {lngserieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        """
        strsqltablename = "T_WC_TMDB_SERIE_IMAGE"
        strsqlupdatecondition = f"ID_SERIE = {lngserieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
        strsqltablename = "T_WC_TMDB_SERIE_VIDEO"
        strsqlupdatecondition = f"ID_SERIE = {lngserieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()
        
        strsqltablename = "T_WC_TMDB_PERSON_SERIE"
        strsqlupdatecondition = f"ID_SERIE = {lngserieid}"
        strsqlupdate = f"DELETE FROM {strsqltablename} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdbseriesetcreditscompleted(lngserieid):
    """
    Mark a TV series' credits as fully processed by setting TIM_CREDITS_COMPLETED timestamp.

    Parameters:
    -----------
    lngserieid : int
        The TMDb TV series ID to update

    Returns:
    --------
    None
    """
    global paris_tz
    global connectioncp

    if lngserieid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_SERIE"
        strsqlupdatecondition = f"ID_SERIE = {lngserieid}"
        strtimcreditscompleted = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        strsqlupdatesetclause = f"TIM_CREDITS_COMPLETED = '{strtimcreditscompleted}', TIM_UPDATED = '{strtimcreditscompleted}'"
        strsqlupdate = f"UPDATE {strsqltablename} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdbseriesetkeywordscompleted(lngserieid):
    """
    Mark a TV series' keywords as fully processed by setting TIM_KEYWORDS_COMPLETED timestamp.

    Parameters:
    -----------
    lngserieid : int
        The TMDb TV series ID to update

    Returns:
    --------
    None
    """
    global paris_tz
    global connectioncp

    if lngserieid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_SERIE"
        strsqlupdatecondition = f"ID_SERIE = {lngserieid}"
        strtimkeywordscompleted = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        strsqlupdatesetclause = f"TIM_KEYWORDS_COMPLETED = '{strtimkeywordscompleted}'"
        strsqlupdate = f"UPDATE {strsqltablename} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdbseriesetwikidatacompleted(lngserieid):
    """
    Mark a TV series' Wikidata integration as completed by setting TIM_WIKIDATA_COMPLETED timestamp.

    Parameters:
    -----------
    lngserieid : int
        The TMDb TV series ID to update

    Returns:
    --------
    None
    """
    global paris_tz
    global connectioncp

    if lngserieid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_SERIE"
        strsqlupdatecondition = f"ID_SERIE = {lngserieid}"
        strtimwikidatacompleted = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        strsqlupdatesetclause = f"TIM_WIKIDATA_COMPLETED = '{strtimwikidatacompleted}'"
        strsqlupdate = f"UPDATE {strsqltablename} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdbseasonsetwikidatacompleted(lngseasonid):
    """
    Mark a TV season's Wikidata integration as completed by setting TIM_WIKIDATA_COMPLETED timestamp.

    Parameters:
    -----------
    lngseasonid : int
        The TMDb TV season ID to update

    Returns:
    --------
    None
    """
    global paris_tz
    global connectioncp

    if lngseasonid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_SEASON"
        strsqlupdatecondition = f"ID_SEASON = {lngseasonid}"
        strtimwikidatacompleted = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        strsqlupdatesetclause = f"TIM_WIKIDATA_COMPLETED = '{strtimwikidatacompleted}'"
        strsqlupdate = f"UPDATE {strsqltablename} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdbepisodesetwikidatacompleted(lngepisodeid):
    """
    Mark a TV episode's Wikidata integration as completed by setting TIM_WIKIDATA_COMPLETED timestamp.

    Parameters:
    -----------
    lngepisodeid : int
        The TMDb TV episode ID to update

    Returns:
    --------
    None
    """
    global paris_tz
    global connectioncp

    if lngepisodeid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_EPISODE"
        strsqlupdatecondition = f"ID_EPISODE = {lngepisodeid}"
        strtimwikidatacompleted = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        strsqlupdatesetclause = f"TIM_WIKIDATA_COMPLETED = '{strtimwikidatacompleted}'"
        strsqlupdate = f"UPDATE {strsqltablename} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def _f_t2ssetwikidatacompleted(strtablename, strpkcolumn, lngid):
    """
    Internal helper. Mark a T2S row's Wikidata integration as completed by setting
    TIM_WIKIDATA_COMPLETED on `strtablename` where `strpkcolumn` = `lngid`.

    The per-table f_t2s*setwikidatacompleted wrappers below exist for symmetry with
    the f_tmdb* helpers so callers in sparql-crawler.py read consistently.
    """
    global paris_tz
    global connectioncp

    if lngid > 0:
        cursor2 = connectioncp.cursor()
        strtimwikidatacompleted = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        strsqlupdate = (
            f"UPDATE {strtablename} "
            f"SET TIM_WIKIDATA_COMPLETED = '{strtimwikidatacompleted}' "
            f"WHERE {strpkcolumn} = {lngid};"
        )
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_t2scollectionsetwikidatacompleted(lngid):
    """Mark a T2S collection's Wikidata integration as completed (scope 118)."""
    _f_t2ssetwikidatacompleted("T_WC_T2S_COLLECTION", "ID_T2S_COLLECTION", lngid)

def f_t2scharactersetwikidatacompleted(lngid):
    """Mark a T2S character's Wikidata integration as completed (scope 119)."""
    _f_t2ssetwikidatacompleted("T_WC_T2S_CHARACTER", "ID_CHARACTER", lngid)

def f_t2sawardsetwikidatacompleted(lngid):
    """Mark a T2S award's Wikidata integration as completed (scope 120)."""
    _f_t2ssetwikidatacompleted("T_WC_T2S_AWARD", "ID_AWARD", lngid)

def f_t2snominationsetwikidatacompleted(lngid):
    """Mark a T2S nomination's Wikidata integration as completed (scope 121)."""
    _f_t2ssetwikidatacompleted("T_WC_T2S_NOMINATION", "ID_NOMINATION", lngid)

def f_t2stopicsetwikidatacompleted(lngid):
    """Mark a T2S topic's Wikidata integration as completed (scope 122)."""
    _f_t2ssetwikidatacompleted("T_WC_T2S_TOPIC", "ID_TOPIC", lngid)

def f_t2stechnicalsetwikidatacompleted(lngid):
    """Mark a T2S technical's Wikidata integration as completed (scope 123)."""
    _f_t2ssetwikidatacompleted("T_WC_T2S_TECHNICAL", "ID_TECHNICAL", lngid)

def f_t2sgroupsetwikidatacompleted(lngid):
    """Mark a T2S group's Wikidata integration as completed (scope 124)."""
    _f_t2ssetwikidatacompleted("T_WC_T2S_GROUP", "ID_GROUP", lngid)

def f_t2smovementsetwikidatacompleted(lngid):
    """Mark a T2S movement's Wikidata integration as completed (scope 125)."""
    _f_t2ssetwikidatacompleted("T_WC_T2S_MOVEMENT", "ID_MOVEMENT", lngid)

def f_t2slistsetwikidatacompleted(lngid):
    """Mark a T2S list's Wikidata integration as completed (scope 126)."""
    _f_t2ssetwikidatacompleted("T_WC_T2S_LIST", "ID_T2S_LIST", lngid)

def f_t2sdeathsetwikidatacompleted(lngid):
    """Mark a T2S death's Wikidata integration as completed (scope 127)."""
    _f_t2ssetwikidatacompleted("T_WC_T2S_DEATH", "ID_DEATH", lngid)

def f_tmdbseriesetwikipediacompleted(lngserieid):
    """
    Mark a TV series' Wikipedia integration as completed by setting TIM_WIKIPEDIA_COMPLETED timestamp.

    Parameters:
    -----------
    lngserieid : int
        The TMDb TV series ID to update

    Returns:
    --------
    None
    """
    global paris_tz
    global connectioncp

    if lngserieid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_SERIE"
        strsqlupdatecondition = f"ID_SERIE = {lngserieid}"
        strtimwikidatacompleted = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        strsqlupdatesetclause = f"TIM_WIKIPEDIA_COMPLETED = '{strtimwikidatacompleted}'"
        strsqlupdate = f"UPDATE {strsqltablename} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdbserieimagestosql(lngserieid):
    """
    Fetch and store images for a TV series from TMDb API.

    Parameters:
    -----------
    lngserieid : int
        The TMDb TV series ID to fetch images for

    Returns:
    --------
    bool
        True if successful, False if failed
    """
    f_tmdbcontentimagesstosql(lngserieid, "tv", "T_WC_TMDB_SERIE", "T_WC_TMDB_SERIE_IMAGE", "ID_SERIE", "POSTER_PATH", "poster", "T_WC_TMDB_SERIE_LANG")

def f_tmdbserievideotosql(lngserieid, strlang):
    """
    Fetch and store videos for a TV series in a specific language from TMDb API.

    Parameters:
    -----------
    lngserieid : int
        The TMDb TV series ID to fetch videos for
    strlang : str
        The language code (e.g., 'en', 'fr')

    Returns:
    --------
    bool
        True if successful, False if failed
    """
    f_tmdbcontentvideosstosql(lngserieid, "tv", "T_WC_TMDB_SERIE", "T_WC_TMDB_SERIE_VIDEO", "ID_SERIE", strlang)

def f_tmdbserietosqleverything(lngserieid):
    """
    Fetch and store complete TV series data including details, credits, keywords, images, and videos.

    Parameters:
    -----------
    lngserieid : int
        The TMDb TV series ID to fetch

    Returns:
    --------
    None
    """
    f_tmdbserietosql(lngserieid)
    f_tmdbserielangtosql(lngserieid,'fr')
    f_tmdbseriesetcreditscompleted(lngserieid)
    f_tmdbseriekeywordstosql(lngserieid)
    f_tmdbseriesetkeywordscompleted(lngserieid)
    f_tmdbserieimagestosql(lngserieid)
    f_tmdbserievideotosql(lngserieid,'en')
    f_tmdbserievideotosql(lngserieid,'fr')

# https://developer.themoviedb.org/reference/tv-season-details

def _f_tmdbparseairdate(strdate):
    """Parse a YYYY-MM-DD air date into (date_str_or_none, year, month, day)."""
    global strdatepattern
    if strdate and re.match(strdatepattern, strdate):
        y, m, d = map(int, strdate.split('-'))
        return strdate, y, m, d
    return None, None, None, None

def f_tmdbseasongetid(lngserieid, lngseasonnumber):
    """Look up the TMDb season id for a given (series id, season number) pair."""
    global connectioncp
    cursor2 = connectioncp.cursor()
    cursor2.execute(
        "SELECT ID_SEASON FROM T_WC_TMDB_SEASON WHERE ID_SERIE = %s AND SEASON_NUMBER = %s",
        (lngserieid, lngseasonnumber)
    )
    row = cursor2.fetchone()
    return int(row['ID_SEASON']) if row else 0

def f_tmdbepisodegetid(lngserieid, lngseasonnumber, lngepisodenumber):
    """Look up the TMDb episode id for a given (series, season, episode) triple."""
    global connectioncp
    cursor2 = connectioncp.cursor()
    cursor2.execute(
        "SELECT ID_EPISODE FROM T_WC_TMDB_EPISODE WHERE ID_SERIE = %s AND SEASON_NUMBER = %s AND EPISODE_NUMBER = %s",
        (lngserieid, lngseasonnumber, lngepisodenumber)
    )
    row = cursor2.fetchone()
    return int(row['ID_EPISODE']) if row else 0

def _f_tmdbepisoderowtosql(lngserieid, lngseasonid, episode):
    """Insert/update one T_WC_TMDB_EPISODE row + embedded crew/guest_stars credits.

    Uses the dict shape returned both inside season['episodes'][...] and by
    /tv/{id}/season/{n}/episode/{m}. Returns the episode TMDb id (0 if missing).
    """
    if not episode or 'id' not in episode:
        return 0

    lngepisodeid = int(episode['id'])
    strairdate, lngyear, lngmonth, lngday = _f_tmdbparseairdate(episode.get('air_date'))

    strtitle = episode.get('name') or ""
    if len(strtitle) > 250:
        strtitle = strtitle[:250]

    arrcouples = {
        "ID_EPISODE": lngepisodeid,
        "ID_SERIE": lngserieid,
        "ID_SEASON": lngseasonid,
        "SEASON_NUMBER": episode.get('season_number'),
        "EPISODE_NUMBER": episode.get('episode_number'),
        "TITLE": strtitle,
        "OVERVIEW": episode.get('overview') or "",
        "AIR_YEAR": lngyear,
        "AIR_MONTH": lngmonth,
        "AIR_DAY": lngday,
        "RUNTIME": episode.get('runtime'),
        "PRODUCTION_CODE": episode.get('production_code') or "",
        "EPISODE_TYPE": episode.get('episode_type') or "",
        "STILL_PATH": episode.get('still_path') or "",
        "VOTE_AVERAGE": episode.get('vote_average', 0),
        "VOTE_COUNT": episode.get('vote_count', 0),
    }
    if strairdate:
        arrcouples["DAT_AIR"] = strairdate

    if 'external_ids' in episode and episode['external_ids']:
        ext = episode['external_ids']
        if ext.get('imdb_id'):
            arrcouples["ID_IMDB"] = ext['imdb_id']
        if ext.get('wikidata_id'):
            arrcouples["ID_WIKIDATA"] = ext['wikidata_id']
        if ext.get('tvdb_id'):
            arrcouples["ID_TVDB"] = ext['tvdb_id']

    cp.f_sqlupdatearray(
        "T_WC_TMDB_EPISODE",
        arrcouples,
        f"ID_EPISODE = {lngepisodeid}",
        1
    )

    # Persist embedded crew + guest_stars (these come with the season payload).
    # Cast for an episode is only reachable via the dedicated episode credits
    # endpoint, so it is handled in f_tmdbepisodetosql.
    _f_tmdbepisodecreditstosql(
        lngserieid, lngseasonid, lngepisodeid,
        cast=None,
        crew=episode.get('crew'),
        guest_stars=episode.get('guest_stars'),
        purge=False
    )
    return lngepisodeid

def _f_tmdbepisodecreditstosql(lngserieid, lngseasonid, lngepisodeid,
                               cast=None, crew=None, guest_stars=None, purge=False):
    """Upsert credits for one episode in T_WC_TMDB_PERSON_EPISODE.

    When purge=True any existing rows for the episode whose ID_CREDIT is not in
    the supplied lists are deleted (used by the dedicated episode credits call,
    which is authoritative). When purge=False rows are merged in (used when the
    data comes embedded in the season payload, which only carries crew + guest
    stars).
    """
    global connectioncp

    arrcredits = []
    if cast:
        for idx, person in enumerate(cast, start=1):
            arrcredits.append(('cast', idx, person))
    if crew:
        for idx, person in enumerate(crew, start=1):
            arrcredits.append(('crew', idx, person))
    if guest_stars:
        for idx, person in enumerate(guest_stars, start=1):
            arrcredits.append(('guest', idx, person))

    seen_credit_ids = []
    for credit_type, lngdisplayorder, person in arrcredits:
        strcreditid = person.get('credit_id')
        lngpersonid = person.get('id')
        if not strcreditid or not lngpersonid:
            continue
        seen_credit_ids.append(strcreditid)

        strcharacter = person.get('character', '') or '' if credit_type in ('cast', 'guest') else ''
        if len(strcharacter) > 600:
            strcharacter = strcharacter[:600]
        strdepartment = person.get('department', '') or '' if credit_type == 'crew' else ''
        strjob = person.get('job', '') or '' if credit_type == 'crew' else ''

        arrrow = {
            "ID_PERSON": lngpersonid,
            "ID_SERIE": lngserieid,
            "ID_SEASON": lngseasonid,
            "ID_EPISODE": lngepisodeid,
            "SEASON_NUMBER": person.get('season_number'),
            "EPISODE_NUMBER": person.get('episode_number'),
            "ID_CREDIT": strcreditid,
            "CAST_CHARACTER": strcharacter,
            "CREW_DEPARTMENT": strdepartment,
            "CREW_JOB": strjob,
            "CREDIT_TYPE": credit_type,
            "DISPLAY_ORDER": person.get('order') if person.get('order') is not None else lngdisplayorder,
        }
        cp.f_sqlupdatearray(
            "T_WC_TMDB_PERSON_EPISODE",
            arrrow,
            f"ID_CREDIT = '{strcreditid}'",
            1
        )

    if purge:
        if seen_credit_ids:
            credit_id_list = "'" + "', '".join(seen_credit_ids) + "'"
            strsqldelete = (
                f"DELETE FROM T_WC_TMDB_PERSON_EPISODE "
                f"WHERE ID_EPISODE = {lngepisodeid} AND ID_CREDIT NOT IN ({credit_id_list})"
            )
        else:
            strsqldelete = f"DELETE FROM T_WC_TMDB_PERSON_EPISODE WHERE ID_EPISODE = {lngepisodeid}"
        cursor2 = connectioncp.cursor()
        cursor2.execute(strsqldelete)
        connectioncp.commit()

def _f_tmdbseasoncreditstosql(lngserieid, lngseasonid, credits_obj, aggregate_obj):
    """Upsert credits for one season in T_WC_TMDB_PERSON_SEASON.

    Prefers TMDb's aggregate_credits (richer: per-role credit_id and
    total_episode_count). If that is absent, falls back to the plain credits
    block. Existing rows for the season whose ID_CREDIT is no longer present
    are removed.
    """
    global connectioncp

    seen_credit_ids = []
    lngdisplayorder = 0

    def _store(credit_type, lngpersonid, strcreditid, strcharacter,
               strdepartment, strjob, lngorder, lngepisodecount):
        nonlocal lngdisplayorder
        if not strcreditid or not lngpersonid:
            return
        seen_credit_ids.append(strcreditid)
        lngdisplayorder += 1
        if strcharacter and len(strcharacter) > 600:
            strcharacter = strcharacter[:600]

        arrrow = {
            "ID_PERSON": lngpersonid,
            "ID_SERIE": lngserieid,
            "ID_SEASON": lngseasonid,
            "ID_CREDIT": strcreditid,
            "CAST_CHARACTER": strcharacter or "",
            "CREW_DEPARTMENT": strdepartment or "",
            "CREW_JOB": strjob or "",
            "CREDIT_TYPE": credit_type,
            "DISPLAY_ORDER": lngorder if lngorder is not None else lngdisplayorder,
            "TOTAL_EPISODE_COUNT": lngepisodecount,
        }
        cp.f_sqlupdatearray(
            "T_WC_TMDB_PERSON_SEASON",
            arrrow,
            f"ID_CREDIT = '{strcreditid}'",
            1
        )

    if aggregate_obj:
        for person in (aggregate_obj.get('cast') or []):
            for role in (person.get('roles') or []):
                _store(
                    'cast',
                    person.get('id'),
                    role.get('credit_id'),
                    role.get('character'),
                    None, None,
                    person.get('order'),
                    role.get('episode_count')
                )
        for person in (aggregate_obj.get('crew') or []):
            for job in (person.get('jobs') or []):
                _store(
                    'crew',
                    person.get('id'),
                    job.get('credit_id'),
                    None,
                    person.get('department') or job.get('department'),
                    job.get('job'),
                    None,
                    job.get('episode_count')
                )
    elif credits_obj:
        for person in (credits_obj.get('cast') or []):
            _store(
                'cast',
                person.get('id'),
                person.get('credit_id'),
                person.get('character'),
                None, None,
                person.get('order'),
                None
            )
        for person in (credits_obj.get('crew') or []):
            _store(
                'crew',
                person.get('id'),
                person.get('credit_id'),
                None,
                person.get('department'),
                person.get('job'),
                None,
                None
            )

    if seen_credit_ids:
        credit_id_list = "'" + "', '".join(seen_credit_ids) + "'"
        strsqldelete = (
            f"DELETE FROM T_WC_TMDB_PERSON_SEASON "
            f"WHERE ID_SEASON = {lngseasonid} AND ID_CREDIT NOT IN ({credit_id_list})"
        )
    else:
        strsqldelete = f"DELETE FROM T_WC_TMDB_PERSON_SEASON WHERE ID_SEASON = {lngseasonid}"
    cursor2 = connectioncp.cursor()
    cursor2.execute(strsqldelete)
    connectioncp.commit()

def f_tmdbseasontosql(lngserieid, lngseasonnumber):
    """
    Fetch a TV season's details, episodes (basic data + crew + guest stars)
    and credits from TMDb and store everything in the database.

    Parameters:
    -----------
    lngserieid : int
        The TMDb TV series ID
    lngseasonnumber : int
        The season number (0 = specials)

    Returns:
    --------
    int
        The TMDb season ID that was stored, or 0 on failure.
    """
    global strtmdbapidomainurl
    global headers
    global strlanguage

    if lngserieid <= 0 or lngseasonnumber is None or lngseasonnumber < 0:
        return 0

    strtmdbapiurl = (
        f"3/tv/{lngserieid}/season/{lngseasonnumber}"
        f"?append_to_response=credits,aggregate_credits,external_ids&language={strlanguage}"
    )
    strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapiurl
    data = f_tmdbfetchjson(strtmdbapifullurl, f"f_tmdbseasontosql({lngserieid},{lngseasonnumber})")
    if data is None:
        return 0
    if data.get('status_code', 0) > 1:
        return 0
    if 'id' not in data:
        return 0

    lngseasonid = int(data['id'])
    strairdate, lngyear, lngmonth, lngday = _f_tmdbparseairdate(data.get('air_date'))

    strtitle = data.get('name') or ""
    if len(strtitle) > 250:
        strtitle = strtitle[:250]

    arrcouples = {
        "ID_SEASON": lngseasonid,
        "ID_SERIE": lngserieid,
        "SEASON_NUMBER": data.get('season_number', lngseasonnumber),
        "TITLE": strtitle,
        "OVERVIEW": data.get('overview') or "",
        "AIR_YEAR": lngyear,
        "AIR_MONTH": lngmonth,
        "AIR_DAY": lngday,
        "POSTER_PATH": data.get('poster_path') or "",
        "EPISODE_COUNT": len(data.get('episodes') or []) or None,
        "VOTE_AVERAGE": data.get('vote_average', 0),
    }
    if strairdate:
        arrcouples["DAT_AIR"] = strairdate

    if 'external_ids' in data and data['external_ids']:
        ext = data['external_ids']
        if ext.get('imdb_id'):
            arrcouples["ID_IMDB"] = ext['imdb_id']
        if ext.get('wikidata_id'):
            arrcouples["ID_WIKIDATA"] = ext['wikidata_id']
        if ext.get('tvdb_id'):
            arrcouples["ID_TVDB"] = ext['tvdb_id']

    cp.f_sqlupdatearray(
        "T_WC_TMDB_SEASON",
        arrcouples,
        f"ID_SEASON = {lngseasonid}",
        1
    )

    # Episodes embedded in the season payload (basic data + crew + guest stars)
    for episode in (data.get('episodes') or []):
        _f_tmdbepisoderowtosql(lngserieid, lngseasonid, episode)

    # Season-level credits (prefer aggregate_credits when present)
    _f_tmdbseasoncreditstosql(
        lngserieid,
        lngseasonid,
        data.get('credits'),
        data.get('aggregate_credits')
    )

    return lngseasonid

def f_tmdbseasonlangtosql(lngserieid, lngseasonnumber, strlang):
    """Fetch and store a season's title/overview translation."""
    global strtmdbapidomainurl
    global headers

    if lngserieid <= 0 or lngseasonnumber is None or lngseasonnumber < 0 or not strlang:
        return

    strtmdbapiurl = f"3/tv/{lngserieid}/season/{lngseasonnumber}?language={strlang}"
    strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapiurl
    data = f_tmdbfetchjson(
        strtmdbapifullurl,
        f"f_tmdbseasonlangtosql({lngserieid},{lngseasonnumber},{strlang})"
    )
    if data is None or data.get('status_code', 0) > 1 or 'id' not in data:
        return

    lngseasonid = int(data['id'])
    strtitle = data.get('name') or ""
    if len(strtitle) > 250:
        strtitle = strtitle[:250]

    arrcouples = {
        "ID_SEASON": lngseasonid,
        "ID_SERIE": lngserieid,
        "LANG": strlang,
        "TITLE": strtitle,
        "OVERVIEW": data.get('overview') or "",
    }
    cp.f_sqlupdatearray(
        "T_WC_TMDB_SEASON_LANG",
        arrcouples,
        f"ID_SEASON = {lngseasonid} AND LANG = '{strlang}'",
        1
    )

def f_tmdbseasonexist(lngserieid, lngseasonnumber):
    """Return False only if TMDb explicitly answers 'not found' (status 34)."""
    global strtmdbapidomainurl
    global headers
    global strlanguage

    if lngserieid <= 0 or lngseasonnumber is None or lngseasonnumber < 0:
        return False

    strtmdbapiurl = f"3/tv/{lngserieid}/season/{lngseasonnumber}?language={strlanguage}"
    strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapiurl
    data = f_tmdbfetchjson(
        strtmdbapifullurl,
        f"f_tmdbseasonexist({lngserieid},{lngseasonnumber})"
    )
    if data is None:
        return True
    return data.get('status_code', 0) != 34

def f_tmdbseasondelete(lngseasonid):
    """Delete a season and its dependent rows (lang, image, video, credits, episodes)."""
    global connectioncp
    if lngseasonid <= 0:
        return

    cursor2 = connectioncp.cursor()

    cursor2.execute(
        "SELECT ID_EPISODE FROM T_WC_TMDB_EPISODE WHERE ID_SEASON = %s",
        (lngseasonid,)
    )
    episode_ids = [row['ID_EPISODE'] for row in cursor2.fetchall()]
    for lngepisodeid in episode_ids:
        f_tmdbepisodedelete(lngepisodeid)

    for tbl in (
        "T_WC_TMDB_PERSON_SEASON",
        "T_WC_TMDB_SEASON_IMAGE",
        "T_WC_TMDB_SEASON_VIDEO",
        "T_WC_TMDB_SEASON_LANG",
        "T_WC_TMDB_SEASON",
    ):
        cursor2.execute(f"DELETE FROM {tbl} WHERE ID_SEASON = %s", (lngseasonid,))
        connectioncp.commit()

def f_tmdbseasonsetcreditscompleted(lngseasonid):
    """Mark a season's credits as fully processed (TIM_CREDITS_COMPLETED)."""
    global paris_tz
    global connectioncp
    if lngseasonid <= 0:
        return
    cursor2 = connectioncp.cursor()
    strnow = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
    cursor2.execute(
        f"UPDATE T_WC_TMDB_SEASON SET TIM_CREDITS_COMPLETED = '{strnow}', "
        f"TIM_UPDATED = '{strnow}' WHERE ID_SEASON = {lngseasonid};"
    )
    connectioncp.commit()

def f_tmdbseasonsettranslationscompleted(lngseasonid):
    """Mark a season's translations as fully processed."""
    global paris_tz
    global connectioncp
    if lngseasonid <= 0:
        return
    cursor2 = connectioncp.cursor()
    strnow = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
    cursor2.execute(
        f"UPDATE T_WC_TMDB_SEASON SET TIM_TRANSLATIONS_COMPLETED = '{strnow}' "
        f"WHERE ID_SEASON = {lngseasonid};"
    )
    connectioncp.commit()

def f_tmdbseasonimagestosql(lngserieid, lngseasonnumber):
    """Fetch and store images for a season."""
    global strtmdbapidomainurl
    global headers
    global connectioncp
    global paris_tz

    if lngserieid <= 0 or lngseasonnumber is None or lngseasonnumber < 0:
        return False

    lngseasonid = f_tmdbseasongetid(lngserieid, lngseasonnumber)
    if lngseasonid <= 0:
        return False

    strtmdbapiurl = f"3/tv/{lngserieid}/season/{lngseasonnumber}/images"
    strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapiurl
    data = f_tmdbfetchjson(
        strtmdbapifullurl,
        f"f_tmdbseasonimagestosql({lngserieid},{lngseasonnumber})"
    )
    if data is None or data.get('status_code', 0) > 1:
        return False

    current_time = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
    current_date = datetime.now(paris_tz).strftime("%Y-%m-%d")

    # Pin the season POSTER_PATH to DISPLAY_ORDER 0 and protect it from cleanup.
    strmainimagepath = ""
    cursormain = connectioncp.cursor()
    cursormain.execute(f"SELECT POSTER_PATH AS MAIN_IMAGE_PATH FROM T_WC_TMDB_SEASON WHERE ID_SEASON = {lngseasonid}")
    rowmain = cursormain.fetchone()
    if rowmain is not None and rowmain.get('MAIN_IMAGE_PATH'):
        strmainimagepath = rowmain['MAIN_IMAGE_PATH']

    all_image_paths = []

    def _store(image_array, image_type):
        lngdisplayorder = 0
        boopintype = (strmainimagepath != "" and image_type == 'poster')
        for image in image_array or []:
            image_path = image.get('file_path', '')
            if not image_path:
                continue
            boothismain = boopintype and image_path == strmainimagepath
            if boothismain:
                lngthisdisplayorder = 0
            else:
                lngdisplayorder += 1
                lngthisdisplayorder = lngdisplayorder
            all_image_paths.append(image_path)
            arrimage = {
                "ID_SEASON": lngseasonid,
                "ID_SERIE": lngserieid,
                "DISPLAY_ORDER": lngthisdisplayorder,
                "DAT_CREAT": current_date,
                "TIM_UPDATED": current_time,
                "TYPE_IMAGE": image_type,
                "LANG": image.get('iso_639_1', ''),
                "IMAGE_PATH": image_path,
                "ASPECT_RATIO": image.get('aspect_ratio', 0),
                "WIDTH": image.get('width', 0),
                "HEIGHT": image.get('height', 0),
                "VOTE_AVERAGE": image.get('vote_average', 0),
                "VOTE_COUNT": image.get('vote_count', 0),
            }
            if boothismain:
                arrimage["DELETED"] = 0
            cp.f_sqlupdatearray(
                "T_WC_TMDB_SEASON_IMAGE",
                arrimage,
                f"ID_SEASON = {lngseasonid} AND TYPE_IMAGE = '{image_type}' "
                f"AND IMAGE_PATH = '{image_path}'",
                1
            )

    _store(data.get('posters'), 'poster')
    _store(data.get('backdrops'), 'backdrop')

    # Guarantee the season poster is present at DISPLAY_ORDER 0 even if the API
    # did not return it, and shield it from the cleanup below.
    if strmainimagepath and strmainimagepath not in all_image_paths:
        all_image_paths.append(strmainimagepath)
        cp.f_sqlupdatearray(
            "T_WC_TMDB_SEASON_IMAGE",
            {
                "ID_SEASON": lngseasonid,
                "ID_SERIE": lngserieid,
                "DISPLAY_ORDER": 0,
                "DELETED": 0,
                "DAT_CREAT": current_date,
                "TIM_UPDATED": current_time,
                "TYPE_IMAGE": 'poster',
                "IMAGE_PATH": strmainimagepath,
            },
            f"ID_SEASON = {lngseasonid} AND TYPE_IMAGE = 'poster' "
            f"AND IMAGE_PATH = '{strmainimagepath}'",
            1
        )

    cursor = connectioncp.cursor()
    if all_image_paths:
        image_paths_list = "'" + "', '".join(all_image_paths) + "'"
        cursor.execute(
            f"DELETE FROM T_WC_TMDB_SEASON_IMAGE WHERE ID_SEASON = {lngseasonid} "
            f"AND IMAGE_PATH NOT IN ({image_paths_list})"
        )
    else:
        cursor.execute(
            f"DELETE FROM T_WC_TMDB_SEASON_IMAGE WHERE ID_SEASON = {lngseasonid}"
        )
    connectioncp.commit()

    cursor.execute(
        f"UPDATE T_WC_TMDB_SEASON SET TIM_IMAGES_COMPLETED = '{current_time}' "
        f"WHERE ID_SEASON = {lngseasonid};"
    )
    connectioncp.commit()
    return True

def f_tmdbseasonvideotosql(lngserieid, lngseasonnumber, strlang):
    """Fetch and store videos for a season in a specific language."""
    global strtmdbapidomainurl
    global headers
    global connectioncp
    global paris_tz

    if lngserieid <= 0 or lngseasonnumber is None or lngseasonnumber < 0 or not strlang:
        return False

    lngseasonid = f_tmdbseasongetid(lngserieid, lngseasonnumber)
    if lngseasonid <= 0:
        return False

    strtmdbapiurl = f"3/tv/{lngserieid}/season/{lngseasonnumber}/videos?language={strlang}"
    strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapiurl
    data = f_tmdbfetchjson(
        strtmdbapifullurl,
        f"f_tmdbseasonvideotosql({lngserieid},{lngseasonnumber},{strlang})"
    )
    if data is None or data.get('status_code', 0) > 1:
        return False

    current_time = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
    current_date = datetime.now(paris_tz).strftime("%Y-%m-%d")
    all_video_ids = []

    lngdisplayorder = 0
    for video in (data.get('results') or []):
        lngdisplayorder += 1
        video_id = video.get('id', '')
        if not video_id:
            continue
        all_video_ids.append(video_id)

        dat_published = None
        published_at_str = video.get('published_at')
        if published_at_str:
            try:
                if published_at_str.endswith('Z'):
                    dt_utc = datetime.strptime(published_at_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.utc)
                else:
                    dt_utc = datetime.fromisoformat(published_at_str)
                    if dt_utc.tzinfo is None:
                        dt_utc = dt_utc.replace(tzinfo=pytz.utc)
                dat_published = dt_utc.astimezone(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                dat_published = None

        arrvideo = {
            "ID_SEASON": lngseasonid,
            "ID_SERIE": lngserieid,
            "DISPLAY_ORDER": lngdisplayorder,
            "DAT_CREAT": current_date,
            "TIM_UPDATED": current_time,
            "DAT_PUBLISHED": dat_published,
            "VIDEO_TYPE": video.get('type', ''),
            "LANG": video.get('iso_639_1', ''),
            "COUNTRY_CODE": video.get('iso_3166_1', ''),
            "ID_CREDIT": video_id,
            "VIDEO_KEY": video.get('key', ''),
            "VIDEO_NAME": video.get('name', ''),
            "VIDEO_SITE": video.get('site', ''),
            "QUALITY": video.get('size', 0),
            "QUALITY_TEXT": str(video.get('size', 0)) + 'p',
            "OFFICIAL": video.get('official', False),
        }
        cp.f_sqlupdatearray(
            "T_WC_TMDB_SEASON_VIDEO",
            arrvideo,
            f"ID_SEASON = {lngseasonid} AND LANG = '{strlang}' AND ID_CREDIT = '{video_id}'",
            1
        )

    cursor = connectioncp.cursor()
    if all_video_ids:
        video_ids_list = "'" + "', '".join(all_video_ids) + "'"
        cursor.execute(
            f"DELETE FROM T_WC_TMDB_SEASON_VIDEO WHERE ID_SEASON = {lngseasonid} "
            f"AND LANG = '{strlang}' AND ID_CREDIT NOT IN ({video_ids_list})"
        )
    else:
        cursor.execute(
            f"DELETE FROM T_WC_TMDB_SEASON_VIDEO WHERE ID_SEASON = {lngseasonid} "
            f"AND LANG = '{strlang}'"
        )
    connectioncp.commit()

    cursor.execute(
        f"UPDATE T_WC_TMDB_SEASON SET TIM_VIDEOS_COMPLETED = '{current_time}' "
        f"WHERE ID_SEASON = {lngseasonid};"
    )
    connectioncp.commit()
    return True

def f_tmdbseasontosqleverything(lngserieid, lngseasonnumber):
    """
    Load complete data for one season: details, episodes (basic + crew + guest
    stars), credits, French translation, images and English/French videos.
    Does NOT recurse into per-episode endpoints. Use
    f_tmdbserieallseasonsepisodestosql() for that.
    """
    lngseasonid = f_tmdbseasontosql(lngserieid, lngseasonnumber)
    if lngseasonid <= 0:
        return 0
    f_tmdbseasonlangtosql(lngserieid, lngseasonnumber, 'fr')
    f_tmdbseasonsettranslationscompleted(lngseasonid)
    f_tmdbseasonsetcreditscompleted(lngseasonid)
    f_tmdbseasonimagestosql(lngserieid, lngseasonnumber)
    f_tmdbseasonvideotosql(lngserieid, lngseasonnumber, 'en')
    f_tmdbseasonvideotosql(lngserieid, lngseasonnumber, 'fr')
    return lngseasonid

# https://developer.themoviedb.org/reference/tv-episode-details

def f_tmdbepisodetosql(lngserieid, lngseasonnumber, lngepisodenumber):
    """
    Fetch a TV episode's full details and credits (cast + crew + guest stars)
    from TMDb and store them. The episode row itself can already exist (created
    from the season payload) — this call enriches it with translations of the
    overview/title in the default language and refreshes credits authoritatively.

    Returns the TMDb episode ID, or 0 on failure.
    """
    global strtmdbapidomainurl
    global headers
    global strlanguage

    if lngserieid <= 0 or lngseasonnumber is None or lngseasonnumber < 0 \
            or lngepisodenumber is None or lngepisodenumber < 0:
        return 0

    strtmdbapiurl = (
        f"3/tv/{lngserieid}/season/{lngseasonnumber}/episode/{lngepisodenumber}"
        f"?append_to_response=credits,external_ids&language={strlanguage}"
    )
    strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapiurl
    data = f_tmdbfetchjson(
        strtmdbapifullurl,
        f"f_tmdbepisodetosql({lngserieid},{lngseasonnumber},{lngepisodenumber})"
    )
    if data is None or data.get('status_code', 0) > 1 or 'id' not in data:
        return 0

    lngseasonid = f_tmdbseasongetid(lngserieid, lngseasonnumber)
    if lngseasonid <= 0:
        # Season row missing — bootstrap it so the episode FK is populated
        lngseasonid = f_tmdbseasontosql(lngserieid, lngseasonnumber)
        if lngseasonid <= 0:
            return 0

    lngepisodeid = _f_tmdbepisoderowtosql(lngserieid, lngseasonid, data)
    if lngepisodeid <= 0:
        return 0

    creds = data.get('credits') or {}
    _f_tmdbepisodecreditstosql(
        lngserieid, lngseasonid, lngepisodeid,
        cast=creds.get('cast'),
        crew=creds.get('crew') or data.get('crew'),
        guest_stars=creds.get('guest_stars') or data.get('guest_stars'),
        purge=True
    )
    return lngepisodeid

def f_tmdbepisodelangtosql(lngserieid, lngseasonnumber, lngepisodenumber, strlang):
    """Fetch and store an episode's translation (title + overview)."""
    global strtmdbapidomainurl
    global headers

    if lngserieid <= 0 or lngseasonnumber is None or lngseasonnumber < 0 \
            or lngepisodenumber is None or lngepisodenumber < 0 or not strlang:
        return

    strtmdbapiurl = (
        f"3/tv/{lngserieid}/season/{lngseasonnumber}/episode/{lngepisodenumber}"
        f"?language={strlang}"
    )
    strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapiurl
    data = f_tmdbfetchjson(
        strtmdbapifullurl,
        f"f_tmdbepisodelangtosql({lngserieid},{lngseasonnumber},{lngepisodenumber},{strlang})"
    )
    if data is None or data.get('status_code', 0) > 1 or 'id' not in data:
        return

    lngepisodeid = int(data['id'])
    lngseasonid = f_tmdbseasongetid(lngserieid, lngseasonnumber)

    strtitle = data.get('name') or ""
    if len(strtitle) > 250:
        strtitle = strtitle[:250]

    arrcouples = {
        "ID_EPISODE": lngepisodeid,
        "ID_SEASON": lngseasonid,
        "ID_SERIE": lngserieid,
        "LANG": strlang,
        "TITLE": strtitle,
        "OVERVIEW": data.get('overview') or "",
    }
    cp.f_sqlupdatearray(
        "T_WC_TMDB_EPISODE_LANG",
        arrcouples,
        f"ID_EPISODE = {lngepisodeid} AND LANG = '{strlang}'",
        1
    )

def f_tmdbepisodeexist(lngserieid, lngseasonnumber, lngepisodenumber):
    """Return False only if TMDb explicitly returns status 34 (not found)."""
    global strtmdbapidomainurl
    global headers
    global strlanguage

    if lngserieid <= 0 or lngseasonnumber is None or lngseasonnumber < 0 \
            or lngepisodenumber is None or lngepisodenumber < 0:
        return False

    strtmdbapiurl = (
        f"3/tv/{lngserieid}/season/{lngseasonnumber}/episode/{lngepisodenumber}"
        f"?language={strlanguage}"
    )
    strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapiurl
    data = f_tmdbfetchjson(
        strtmdbapifullurl,
        f"f_tmdbepisodeexist({lngserieid},{lngseasonnumber},{lngepisodenumber})"
    )
    if data is None:
        return True
    return data.get('status_code', 0) != 34

def f_tmdbepisodedelete(lngepisodeid):
    """Delete an episode and its dependent rows."""
    global connectioncp
    if lngepisodeid <= 0:
        return
    cursor2 = connectioncp.cursor()
    for tbl in (
        "T_WC_TMDB_PERSON_EPISODE",
        "T_WC_TMDB_EPISODE_IMAGE",
        "T_WC_TMDB_EPISODE_VIDEO",
        "T_WC_TMDB_EPISODE_LANG",
        "T_WC_TMDB_EPISODE",
    ):
        cursor2.execute(f"DELETE FROM {tbl} WHERE ID_EPISODE = %s", (lngepisodeid,))
        connectioncp.commit()

def f_tmdbepisodesetcreditscompleted(lngepisodeid):
    """Mark an episode's credits as fully processed."""
    global paris_tz
    global connectioncp
    if lngepisodeid <= 0:
        return
    cursor2 = connectioncp.cursor()
    strnow = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
    cursor2.execute(
        f"UPDATE T_WC_TMDB_EPISODE SET TIM_CREDITS_COMPLETED = '{strnow}', "
        f"TIM_UPDATED = '{strnow}' WHERE ID_EPISODE = {lngepisodeid};"
    )
    connectioncp.commit()

def f_tmdbepisodesettranslationscompleted(lngepisodeid):
    """Mark an episode's translations as fully processed."""
    global paris_tz
    global connectioncp
    if lngepisodeid <= 0:
        return
    cursor2 = connectioncp.cursor()
    strnow = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
    cursor2.execute(
        f"UPDATE T_WC_TMDB_EPISODE SET TIM_TRANSLATIONS_COMPLETED = '{strnow}' "
        f"WHERE ID_EPISODE = {lngepisodeid};"
    )
    connectioncp.commit()

def f_tmdbepisodeimagestosql(lngserieid, lngseasonnumber, lngepisodenumber):
    """Fetch and store images (stills) for an episode."""
    global strtmdbapidomainurl
    global headers
    global connectioncp
    global paris_tz

    if lngserieid <= 0 or lngseasonnumber is None or lngseasonnumber < 0 \
            or lngepisodenumber is None or lngepisodenumber < 0:
        return False

    lngepisodeid = f_tmdbepisodegetid(lngserieid, lngseasonnumber, lngepisodenumber)
    lngseasonid = f_tmdbseasongetid(lngserieid, lngseasonnumber)
    if lngepisodeid <= 0 or lngseasonid <= 0:
        return False

    strtmdbapiurl = (
        f"3/tv/{lngserieid}/season/{lngseasonnumber}/episode/{lngepisodenumber}/images"
    )
    strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapiurl
    data = f_tmdbfetchjson(
        strtmdbapifullurl,
        f"f_tmdbepisodeimagestosql({lngserieid},{lngseasonnumber},{lngepisodenumber})"
    )
    if data is None or data.get('status_code', 0) > 1:
        return False

    current_time = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
    current_date = datetime.now(paris_tz).strftime("%Y-%m-%d")

    # Pin the episode STILL_PATH to DISPLAY_ORDER 0 and protect it from cleanup.
    strmainimagepath = ""
    cursormain = connectioncp.cursor()
    cursormain.execute(f"SELECT STILL_PATH AS MAIN_IMAGE_PATH FROM T_WC_TMDB_EPISODE WHERE ID_EPISODE = {lngepisodeid}")
    rowmain = cursormain.fetchone()
    if rowmain is not None and rowmain.get('MAIN_IMAGE_PATH'):
        strmainimagepath = rowmain['MAIN_IMAGE_PATH']

    all_image_paths = []

    lngdisplayorder = 0
    for image in (data.get('stills') or []):
        image_path = image.get('file_path', '')
        if not image_path:
            continue
        boothismain = strmainimagepath != "" and image_path == strmainimagepath
        if boothismain:
            lngthisdisplayorder = 0
        else:
            lngdisplayorder += 1
            lngthisdisplayorder = lngdisplayorder
        all_image_paths.append(image_path)
        arrimage = {
            "ID_EPISODE": lngepisodeid,
            "ID_SEASON": lngseasonid,
            "ID_SERIE": lngserieid,
            "DISPLAY_ORDER": lngthisdisplayorder,
            "DAT_CREAT": current_date,
            "TIM_UPDATED": current_time,
            "TYPE_IMAGE": 'still',
            "LANG": image.get('iso_639_1', ''),
            "IMAGE_PATH": image_path,
            "ASPECT_RATIO": image.get('aspect_ratio', 0),
            "WIDTH": image.get('width', 0),
            "HEIGHT": image.get('height', 0),
            "VOTE_AVERAGE": image.get('vote_average', 0),
            "VOTE_COUNT": image.get('vote_count', 0),
        }
        if boothismain:
            arrimage["DELETED"] = 0
        cp.f_sqlupdatearray(
            "T_WC_TMDB_EPISODE_IMAGE",
            arrimage,
            f"ID_EPISODE = {lngepisodeid} AND TYPE_IMAGE = 'still' "
            f"AND IMAGE_PATH = '{image_path}'",
            1
        )

    # Guarantee the episode still is present at DISPLAY_ORDER 0 even if the API
    # did not return it, and shield it from the cleanup below.
    if strmainimagepath and strmainimagepath not in all_image_paths:
        all_image_paths.append(strmainimagepath)
        cp.f_sqlupdatearray(
            "T_WC_TMDB_EPISODE_IMAGE",
            {
                "ID_EPISODE": lngepisodeid,
                "ID_SEASON": lngseasonid,
                "ID_SERIE": lngserieid,
                "DISPLAY_ORDER": 0,
                "DELETED": 0,
                "DAT_CREAT": current_date,
                "TIM_UPDATED": current_time,
                "TYPE_IMAGE": 'still',
                "IMAGE_PATH": strmainimagepath,
            },
            f"ID_EPISODE = {lngepisodeid} AND TYPE_IMAGE = 'still' "
            f"AND IMAGE_PATH = '{strmainimagepath}'",
            1
        )

    cursor = connectioncp.cursor()
    if all_image_paths:
        image_paths_list = "'" + "', '".join(all_image_paths) + "'"
        cursor.execute(
            f"DELETE FROM T_WC_TMDB_EPISODE_IMAGE WHERE ID_EPISODE = {lngepisodeid} "
            f"AND IMAGE_PATH NOT IN ({image_paths_list})"
        )
    else:
        cursor.execute(
            f"DELETE FROM T_WC_TMDB_EPISODE_IMAGE WHERE ID_EPISODE = {lngepisodeid}"
        )
    connectioncp.commit()

    cursor.execute(
        f"UPDATE T_WC_TMDB_EPISODE SET TIM_IMAGES_COMPLETED = '{current_time}' "
        f"WHERE ID_EPISODE = {lngepisodeid};"
    )
    connectioncp.commit()
    return True

def f_tmdbepisodevideotosql(lngserieid, lngseasonnumber, lngepisodenumber, strlang):
    """Fetch and store videos for an episode in a specific language."""
    global strtmdbapidomainurl
    global headers
    global connectioncp
    global paris_tz

    if lngserieid <= 0 or lngseasonnumber is None or lngseasonnumber < 0 \
            or lngepisodenumber is None or lngepisodenumber < 0 or not strlang:
        return False

    lngepisodeid = f_tmdbepisodegetid(lngserieid, lngseasonnumber, lngepisodenumber)
    lngseasonid = f_tmdbseasongetid(lngserieid, lngseasonnumber)
    if lngepisodeid <= 0 or lngseasonid <= 0:
        return False

    strtmdbapiurl = (
        f"3/tv/{lngserieid}/season/{lngseasonnumber}/episode/{lngepisodenumber}/videos"
        f"?language={strlang}"
    )
    strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapiurl
    data = f_tmdbfetchjson(
        strtmdbapifullurl,
        f"f_tmdbepisodevideotosql({lngserieid},{lngseasonnumber},{lngepisodenumber},{strlang})"
    )
    if data is None or data.get('status_code', 0) > 1:
        return False

    current_time = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
    current_date = datetime.now(paris_tz).strftime("%Y-%m-%d")
    all_video_ids = []

    lngdisplayorder = 0
    for video in (data.get('results') or []):
        lngdisplayorder += 1
        video_id = video.get('id', '')
        if not video_id:
            continue
        all_video_ids.append(video_id)

        dat_published = None
        published_at_str = video.get('published_at')
        if published_at_str:
            try:
                if published_at_str.endswith('Z'):
                    dt_utc = datetime.strptime(published_at_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.utc)
                else:
                    dt_utc = datetime.fromisoformat(published_at_str)
                    if dt_utc.tzinfo is None:
                        dt_utc = dt_utc.replace(tzinfo=pytz.utc)
                dat_published = dt_utc.astimezone(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                dat_published = None

        arrvideo = {
            "ID_EPISODE": lngepisodeid,
            "ID_SEASON": lngseasonid,
            "ID_SERIE": lngserieid,
            "DISPLAY_ORDER": lngdisplayorder,
            "DAT_CREAT": current_date,
            "TIM_UPDATED": current_time,
            "DAT_PUBLISHED": dat_published,
            "VIDEO_TYPE": video.get('type', ''),
            "LANG": video.get('iso_639_1', ''),
            "COUNTRY_CODE": video.get('iso_3166_1', ''),
            "ID_CREDIT": video_id,
            "VIDEO_KEY": video.get('key', ''),
            "VIDEO_NAME": video.get('name', ''),
            "VIDEO_SITE": video.get('site', ''),
            "QUALITY": video.get('size', 0),
            "QUALITY_TEXT": str(video.get('size', 0)) + 'p',
            "OFFICIAL": video.get('official', False),
        }
        cp.f_sqlupdatearray(
            "T_WC_TMDB_EPISODE_VIDEO",
            arrvideo,
            f"ID_EPISODE = {lngepisodeid} AND LANG = '{strlang}' AND ID_CREDIT = '{video_id}'",
            1
        )

    cursor = connectioncp.cursor()
    if all_video_ids:
        video_ids_list = "'" + "', '".join(all_video_ids) + "'"
        cursor.execute(
            f"DELETE FROM T_WC_TMDB_EPISODE_VIDEO WHERE ID_EPISODE = {lngepisodeid} "
            f"AND LANG = '{strlang}' AND ID_CREDIT NOT IN ({video_ids_list})"
        )
    else:
        cursor.execute(
            f"DELETE FROM T_WC_TMDB_EPISODE_VIDEO WHERE ID_EPISODE = {lngepisodeid} "
            f"AND LANG = '{strlang}'"
        )
    connectioncp.commit()

    cursor.execute(
        f"UPDATE T_WC_TMDB_EPISODE SET TIM_VIDEOS_COMPLETED = '{current_time}' "
        f"WHERE ID_EPISODE = {lngepisodeid};"
    )
    connectioncp.commit()
    return True

def f_tmdbepisodetosqleverything(lngserieid, lngseasonnumber, lngepisodenumber):
    """Load complete data for one episode: details, credits, French translation,
    images and English/French videos."""
    lngepisodeid = f_tmdbepisodetosql(lngserieid, lngseasonnumber, lngepisodenumber)
    if lngepisodeid <= 0:
        return 0
    f_tmdbepisodelangtosql(lngserieid, lngseasonnumber, lngepisodenumber, 'fr')
    f_tmdbepisodesettranslationscompleted(lngepisodeid)
    f_tmdbepisodesetcreditscompleted(lngepisodeid)
    f_tmdbepisodeimagestosql(lngserieid, lngseasonnumber, lngepisodenumber)
    f_tmdbepisodevideotosql(lngserieid, lngseasonnumber, lngepisodenumber, 'en')
    f_tmdbepisodevideotosql(lngserieid, lngseasonnumber, lngepisodenumber, 'fr')
    return lngepisodeid

def f_tmdbseriechangesget(lngserieid, strstartdate=None):
    """
    GET /tv/{id}/changes?start_date=YYYY-MM-DD.

    Returns the parsed `changes[]` list (each item is a dict with `key` and
    `items[]`) or None on HTTP/JSON failure / TMDb error. Returns [] when
    TMDb reports no changes in the window.

    `strstartdate` must be within the last 14 days; caller is responsible for
    deciding whether to pass None (= full window, but TMDb still caps at 14d).
    """
    global strtmdbapidomainurl
    if lngserieid <= 0:
        return None
    strurl = strtmdbapidomainurl + "/3/tv/" + str(lngserieid) + "/changes"
    if strstartdate:
        strurl += "?start_date=" + strstartdate
    data = f_tmdbfetchjson(strurl, f"f_tmdbseriechangesget({lngserieid})")
    if data is None:
        return None
    if data.get('status_code', 0) > 1:
        return None
    return data.get('changes') or []


def _f_tmdbseriestamplastchangescheck(lngserieid):
    """Stamp T_WC_TMDB_SERIE.TIM_LAST_CHANGES_CHECK = now."""
    global paris_tz
    global connectioncp
    if lngserieid <= 0:
        return
    strnow = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
    cursor2 = connectioncp.cursor()
    cursor2.execute(
        f"UPDATE T_WC_TMDB_SERIE SET TIM_LAST_CHANGES_CHECK = '{strnow}', "
        f"TIM_UPDATED = '{strnow}' WHERE ID_SERIE = {lngserieid};"
    )
    connectioncp.commit()


def _f_tmdbseriestampchildrencompleted(lngserieid, strcolumn):
    """Stamp a children-completion column on the series row (TIM_SEASONS_COMPLETED
    or TIM_EPISODES_COMPLETED)."""
    global paris_tz
    global connectioncp
    if lngserieid <= 0:
        return
    strnow = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
    cursor2 = connectioncp.cursor()
    cursor2.execute(
        f"UPDATE T_WC_TMDB_SERIE SET {strcolumn} = '{strnow}', "
        f"TIM_UPDATED = '{strnow}' WHERE ID_SERIE = {lngserieid};"
    )
    connectioncp.commit()


def f_tmdbserieselectiveseasonsepisodestosql(lngserieid):
    """
    Selective season+episode refresh driven by /tv/{id}/changes.
    Implements the four-tier strategy documented in SERIE_UPDATE.md.

    Assumes f_tmdbserietosqleverything has already been called for this series
    (series row + activity signals up to date). Side effects:
      - Refreshes only the seasons/episodes selected by the rules.
      - Stamps TIM_LAST_CHANGES_CHECK on success.
      - Stamps TIM_SEASONS_COMPLETED / TIM_EPISODES_COMPLETED when the selected
        children all completed successfully.
    """
    global strtmdbapidomainurl
    global connectioncp

    if lngserieid <= 0:
        return

    cursor2 = connectioncp.cursor()
    cursor2.execute(
        "SELECT TIM_LAST_CHANGES_CHECK, IN_PRODUCTION, "
        "       NEXT_EPISODE_DAT_AIR, LAST_EPISODE_DAT_AIR "
        "FROM T_WC_TMDB_SERIE WHERE ID_SERIE = %s",
        (lngserieid,)
    )
    serierow = cursor2.fetchone()
    if not serierow:
        print(f"f_tmdbserieselectiveseasonsepisodestosql: series {lngserieid} not in DB")
        return

    today = datetime.now().date()

    # --- T1: ask TMDb what changed ---
    strstartdate = None
    timlastcheck = serierow['TIM_LAST_CHANGES_CHECK']
    if timlastcheck is not None:
        gapdays = (datetime.now() - timlastcheck).days
        if 0 <= gapdays <= INT_TMDB_CHANGES_MAX_DAYS:
            strstartdate = timlastcheck.strftime('%Y-%m-%d')

    arrchanges = None
    if strstartdate is not None:
        arrchanges = f_tmdbseriechangesget(lngserieid, strstartdate)
    # arrchanges interpretation:
    #   None        — no usable T1 info (NULL/gap>14d/HTTP error) → apply full rules
    #   []          — TMDb reports nothing changed
    #   [items...]  — list of {key, items[]}

    booseasonsignaled = False
    arrepisodehintsmap = {}  # {season_number: set(episode_numbers)}
    if arrchanges:
        for change in arrchanges:
            strkey = change.get('key', '')
            if strkey == 'seasons':
                booseasonsignaled = True
            elif strkey == 'episodes':
                arritems = change.get('items') or []
                for item in arritems:
                    arrval = item.get('value') or {}
                    snum = arrval.get('season_number')
                    enum = arrval.get('episode_number')
                    if snum is not None and enum is not None:
                        arrepisodehintsmap.setdefault(int(snum), set()).add(int(enum))

    booneedseasonscan = (
        arrchanges is None
        or booseasonsignaled
        or bool(arrepisodehintsmap)
    )
    if not booneedseasonscan:
        # Stable series, nothing relevant changed — just record the check time.
        _f_tmdbseriestamplastchangescheck(lngserieid)
        return

    # --- T2: snapshot /tv/{id} for seasons[] ---
    strtmdbapifullurl = strtmdbapidomainurl + "/3/tv/" + str(lngserieid)
    seriedata = f_tmdbfetchjson(strtmdbapifullurl, f"f_tmdbserieselectiveseasonsepisodestosql({lngserieid})")
    if seriedata is None or seriedata.get('status_code', 0) > 1:
        return
    arrtmdbseasons = seriedata.get('seasons') or []

    # "Active" judgment from persisted signals.
    booserieactive = False
    if serierow['IN_PRODUCTION'] == 1:
        booserieactive = True
    elif serierow['NEXT_EPISODE_DAT_AIR'] is not None:
        booserieactive = True
    elif serierow['LAST_EPISODE_DAT_AIR'] is not None:
        if (today - serierow['LAST_EPISODE_DAT_AIR']).days <= INT_ACTIVE_SERIES_LOOKBACK_DAYS:
            booserieactive = True

    lnglatestseasonnumber = None
    if booserieactive and arrtmdbseasons:
        arrcand = [s.get('season_number') for s in arrtmdbseasons
                   if s.get('season_number') is not None and (s.get('season_number') or 0) > 0]
        if arrcand:
            lnglatestseasonnumber = max(arrcand)

    # --- T3 selection ---
    arrseasonselections = []  # list of (season_number, reason)
    for tmdbseason in arrtmdbseasons:
        snum = tmdbseason.get('season_number')
        if snum is None:
            continue
        snum = int(snum)
        strreason = None
        lnglocalseasonid = f_tmdbseasongetid(lngserieid, snum)
        if lnglocalseasonid == 0:
            strreason = "missing locally"
        else:
            cursor3 = connectioncp.cursor()
            cursor3.execute(
                "SELECT EPISODE_COUNT, DAT_AIR, "
                "       TIM_CREDITS_COMPLETED, TIM_TRANSLATIONS_COMPLETED, "
                "       TIM_IMAGES_COMPLETED, TIM_VIDEOS_COMPLETED "
                "FROM T_WC_TMDB_SEASON WHERE ID_SEASON = %s",
                (lnglocalseasonid,)
            )
            seasonrow = cursor3.fetchone()
            if not seasonrow:
                strreason = "local row missing"
            else:
                tmdbepcount = tmdbseason.get('episode_count')
                localepcount = seasonrow['EPISODE_COUNT']
                if tmdbepcount is not None and localepcount != tmdbepcount:
                    strreason = f"episode_count drift ({localepcount} -> {tmdbepcount})"
                elif (seasonrow['TIM_CREDITS_COMPLETED'] is None or
                      seasonrow['TIM_TRANSLATIONS_COMPLETED'] is None or
                      seasonrow['TIM_IMAGES_COMPLETED'] is None or
                      seasonrow['TIM_VIDEOS_COMPLETED'] is None):
                    strreason = "season completion incomplete"
                elif seasonrow['DAT_AIR'] is not None:
                    intagedays = (today - seasonrow['DAT_AIR']).days
                    if 0 <= intagedays <= INT_RECENT_SEASON_DAYS:
                        strreason = f"recent season ({intagedays}d)"
                if strreason is None and snum == lnglatestseasonnumber:
                    strreason = "active series, latest season"
        if strreason is None and snum in arrepisodehintsmap:
            strreason = "T1 episodes hint"
        if strreason is not None:
            arrseasonselections.append((snum, strreason))

    # --- T3: refresh selected seasons ---
    arrrefreshedseasonnumbers = set()
    booallseasonsok = True
    for snum, strreason in arrseasonselections:
        print(f"  selective: refresh season {snum} ({strreason})")
        lngseasonid = f_tmdbseasontosqleverything(lngserieid, snum)
        if lngseasonid > 0:
            arrrefreshedseasonnumbers.add(snum)
        else:
            booallseasonsok = False

    # --- T4 selection + refresh ---
    booallepisodesok = True
    boohadepisodework = False
    for snum in arrrefreshedseasonnumbers:
        lngseasonid = f_tmdbseasongetid(lngserieid, snum)
        if lngseasonid == 0:
            continue
        cursor3 = connectioncp.cursor()
        cursor3.execute(
            "SELECT EPISODE_NUMBER, DAT_AIR, "
            "       TIM_CREDITS_COMPLETED, TIM_TRANSLATIONS_COMPLETED, "
            "       TIM_IMAGES_COMPLETED, TIM_VIDEOS_COMPLETED "
            "FROM T_WC_TMDB_EPISODE WHERE ID_SEASON = %s ORDER BY EPISODE_NUMBER",
            (lngseasonid,)
        )
        arrepisodes = cursor3.fetchall()
        snhints = arrepisodehintsmap.get(snum, set())
        booactiveseason = (snum == lnglatestseasonnumber)
        for eprow in arrepisodes:
            enum = eprow['EPISODE_NUMBER']
            if enum is None:
                continue
            enum = int(enum)
            strepreason = None
            if enum in snhints:
                strepreason = "T1 episodes hint"
            elif (eprow['TIM_CREDITS_COMPLETED'] is None or
                  eprow['TIM_TRANSLATIONS_COMPLETED'] is None or
                  eprow['TIM_IMAGES_COMPLETED'] is None or
                  eprow['TIM_VIDEOS_COMPLETED'] is None):
                strepreason = "episode completion incomplete"
            elif eprow['DAT_AIR'] is not None:
                intepagedays = (today - eprow['DAT_AIR']).days
                if 0 <= intepagedays <= INT_RECENT_EPISODE_DAYS:
                    strepreason = f"recent episode ({intepagedays}d)"
            if strepreason is None and booactiveseason:
                strepreason = "active season"
            if strepreason is not None:
                boohadepisodework = True
                print(f"    selective: refresh S{snum:02d}E{enum:02d} ({strepreason})")
                lngepid = f_tmdbepisodetosqleverything(lngserieid, snum, enum)
                if lngepid <= 0:
                    booallepisodesok = False

    # --- Completion stamps ---
    _f_tmdbseriestamplastchangescheck(lngserieid)
    if arrseasonselections and booallseasonsok:
        _f_tmdbseriestampchildrencompleted(lngserieid, "TIM_SEASONS_COMPLETED")
    if boohadepisodework and booallepisodesok:
        _f_tmdbseriestampchildrencompleted(lngserieid, "TIM_EPISODES_COMPLETED")


def f_tmdbserieallseasonsepisodestosql(lngserieid, intloadepisodes=1):
    """
    For a given series, load every season (with embedded episode basic data
    and credits). When intloadepisodes=1 (default) also call the per-episode
    endpoint for each episode to capture cast credits, translations, images
    and videos.

    Reads number_of_seasons from the TMDb API to know how many to iterate.

    Bootstrap-only / operator-backfill entrypoint. The TMDb-changes loop
    uses f_tmdbserieselectiveseasonsepisodestosql instead.
    """
    global strtmdbapidomainurl
    global headers
    global strlanguage
    global connectioncp

    if lngserieid <= 0:
        return

    strtmdbapiurl = f"3/tv/{lngserieid}?language={strlanguage}"
    strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapiurl
    data = f_tmdbfetchjson(strtmdbapifullurl, f"f_tmdbserieallseasonsepisodestosql({lngserieid})")
    if data is None or data.get('status_code', 0) > 1:
        return

    arrseasons = data.get('seasons') or []
    if not arrseasons:
        # Fall back to number_of_seasons + a 0 special season
        lngnumseasons = data.get('number_of_seasons', 0) or 0
        arrseasons = [{"season_number": n} for n in range(0, lngnumseasons + 1)]

    for season in arrseasons:
        lngseasonnumber = season.get('season_number')
        if lngseasonnumber is None:
            continue
        lngseasonid = f_tmdbseasontosqleverything(lngserieid, lngseasonnumber)
        if lngseasonid <= 0:
            continue
        if intloadepisodes != 1:
            continue
        cursor2 = connectioncp.cursor()
        cursor2.execute(
            "SELECT EPISODE_NUMBER FROM T_WC_TMDB_EPISODE "
            "WHERE ID_SEASON = %s ORDER BY EPISODE_NUMBER",
            (lngseasonid,)
        )
        episode_numbers = [row['EPISODE_NUMBER'] for row in cursor2.fetchall() if row['EPISODE_NUMBER'] is not None]
        for lngepisodenumber in episode_numbers:
            f_tmdbepisodetosqleverything(lngserieid, lngseasonnumber, lngepisodenumber)

# https://developer.themoviedb.org/reference/collection-details

def f_tmdbcollectiontosql(lngcollectionid):
    """
    Fetch collection details from TMDb API and store in database.

    Parameters:
    -----------
    lngcollectionid : int
        The TMDb collection ID to fetch

    Returns:
    --------
    None
    """
    global strtmdbapidomainurl
    global headers
    global strlanguagecountry
    global strlanguage

    if lngcollectionid > 0:
        strtmdbapicollectionurl = "3/collection/" + str(lngcollectionid) + "?language=" + strlanguagecountry
        strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapicollectionurl
        # print(strtmdbapifullurl)
        data = f_tmdbfetchjson(strtmdbapifullurl, f"f_tmdbcollectiontosql({lngcollectionid})")
        if data is None:
            return
        
        #strapicollectionfordb = json.dumps(data, ensure_ascii=False)
        
        # print(response.json)
        lngcollectionstatuscode = 0
        if 'status_code' in data:
            lngcollectionstatuscode = data['status_code']
        if lngcollectionstatuscode <= 1:
            # API request result is not an error
            # Extract data using the keys
            strcollectionoverview = ""
            if 'overview' in data:
                strcollectionoverview = data['overview']
            strcollectionposterpath = ""
            if 'poster_path' in data:
                strcollectionposterpath = data['poster_path']
            strcollectionname = ""
            if 'name' in data:
                strcollectionname = data['name']
            strcollectionbackdroppath = ""
            if 'backdrop_path' in data:
                strcollectionbackdroppath = data['backdrop_path']
            
            # print(f"{strcollectionname}")
            # print(f"{strcollectionbackdroppath}")
            # print(f"Overview: {strcollectionoverview}")
            
            arrcollectioncouples = {}
            arrcollectioncouples["ID_COLLECTION"] = lngcollectionid
            #arrcollectioncouples["API_URL"] = strtmdbapicollectionurl
            #arrcollectioncouples["CRAWLER_VERSION"] = 1
            #arrcollectioncouples["API_RESULT"] = strapicollectionfordb
            arrcollectioncouples["OVERVIEW"] = strcollectionoverview
            arrcollectioncouples["POSTER_PATH"] = strcollectionposterpath
            if strcollectionname != "":
                arrcollectioncouples["NAME"] = strcollectionname
            arrcollectioncouples["BACKDROP_PATH"] = strcollectionbackdroppath
            
            strsqltablename = "T_WC_TMDB_COLLECTION"
            strsqlupdatecondition = f"ID_COLLECTION = {lngcollectionid}"
            cp.f_sqlupdatearray(strsqltablename,arrcollectioncouples,strsqlupdatecondition,1)

def f_tmdbcollectionlangtosql(lngcollectionid, strlang):
    """
    Fetch collection details in a specific language from TMDb API and store in database.

    Parameters:
    -----------
    lngcollectionid : int
        The TMDb collection ID to fetch
    strlang : str
        The language code (e.g., 'fr', 'de', 'es')

    Returns:
    --------
    None
    """
    global strtmdbapidomainurl
    global headers
    global strlanguagecountry
    global strlanguage
    global strdatepattern
    global connectioncp

    if lngcollectionid > 0:
        # New TMDb API call with append_to_response since 2024-05-24 10:00
        strtmdbapicollectionurl = "3/collection/" + str(lngcollectionid) + "?language=" + strlang
        strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapicollectionurl
        # print(strtmdbapifullurl)
        data = f_tmdbfetchjson(strtmdbapifullurl, f"f_tmdbcollectionlangtosql({lngcollectionid}, {strlang})")
        if data is None:
            return
        lngcollectionstatuscode = 0
        if 'status_code' in data:
            lngcollectionstatuscode = data['status_code']
        if lngcollectionstatuscode <= 1:
            # API request result is not an error
            # Extract data using the keys
            strcollectionoverview = ""
            if 'overview' in data:
                strcollectionoverview = data['overview']
            strcollectionposterpath = ""
            if 'poster_path' in data:
                strcollectionposterpath = data['poster_path']
            strcollectionname = ""
            if 'name' in data:
                strcollectionname = data['name']
            strcollectionbackdroppath = ""
            if 'backdrop_path' in data:
                strcollectionbackdroppath = data['backdrop_path']
            
            if strcollectionname:
                if len(strcollectionname) > 250:
                    # If title is too long, we chop it
                    strcollectionname = strcollectionname[:250]
            
            arrcollectioncouples = {}
            #arrcollectioncouples["API_URL"] = strtmdbapicollectionurl
            arrcollectioncouples["OVERVIEW"] = strcollectionoverview
            arrcollectioncouples["ID_COLLECTION"] = lngcollectionid
            arrcollectioncouples["LANG"] = strlang
            arrcollectioncouples["POSTER_PATH"] = strcollectionposterpath
            if strcollectionname != "":
                arrcollectioncouples["NAME"] = strcollectionname
            arrcollectioncouples["BACKDROP_PATH"] = strcollectionbackdroppath
            
            if 'overview' in data:
                encoded_overview = data['overview'].replace('\n', '\\n').replace('"', '\\"')
                data['overview'] = encoded_overview
            #strapicollectionfordb = json.dumps(data, ensure_ascii=False)
            #arrcollectioncouples["API_RESULT"] = strapicollectionfordb
            #arrcollectioncouples["CRAWLER_VERSION"] = 3
            
            strsqltablename = "T_WC_TMDB_COLLECTION_LANG"
            strsqlupdatecondition = f"ID_COLLECTION = {lngcollectionid} AND LANG = '{strlang}'"
            cp.f_sqlupdatearray(strsqltablename,arrcollectioncouples,strsqlupdatecondition,1)

def f_tmdbcollectionsetcreditscompleted(lngcollectionid):
    """
    Mark a collection as fully processed by setting TIM_CREDITS_COMPLETED timestamp.

    Parameters:
    -----------
    lngcollectionid : int
        The TMDb collection ID to update

    Returns:
    --------
    None
    """
    global paris_tz
    global connectioncp

    if lngcollectionid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_COLLECTION"
        strsqlupdatecondition = f"ID_COLLECTION = {lngcollectionid}"
        strtimcreditscompleted = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        strsqlupdatesetclause = f"TIM_CREDITS_COMPLETED = '{strtimcreditscompleted}', TIM_UPDATED = '{strtimcreditscompleted}'"
        strsqlupdate = f"UPDATE {strsqltablename} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdbcollectionimagestosql(lngcollectionid):
    """
    Fetch and store images for a collection from TMDb API.

    Parameters:
    -----------
    lngcollectionid : int
        The TMDb collection ID to fetch images for

    Returns:
    --------
    bool
        True if successful, False if failed
    """
    f_tmdbcontentimagesstosql(lngcollectionid, "collection", "T_WC_TMDB_COLLECTION", "T_WC_TMDB_COLLECTION_IMAGE", "ID_COLLECTION", "POSTER_PATH", "poster", "T_WC_TMDB_COLLECTION_LANG")

def f_tmdbcollectiontosqleverything(lngcollectionid):
    """
    Fetch and store complete collection data including details and images.

    Parameters:
    -----------
    lngcollectionid : int
        The TMDb collection ID to fetch

    Returns:
    --------
    None
    """
    f_tmdbcollectiontosql(lngcollectionid)
    f_tmdbcollectionlangtosql(lngcollectionid,'fr')
    f_tmdbcollectionsetcreditscompleted(lngcollectionid)
    f_tmdbcollectionimagestosql(lngcollectionid)

def f_tmdbcompanytosql(lngcompanyid):
    """
    Fetch company details from TMDb API and store in database.

    Parameters:
    -----------
    lngcompanyid : int
        The TMDb company ID to fetch

    Returns:
    --------
    None
    """
    global strtmdbapidomainurl
    global headers
    global strlanguagecountry
    global strlanguage

    if lngcompanyid > 0:
        strtmdbapicompanyurl = "3/company/" + str(lngcompanyid)
        strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapicompanyurl
        data = f_tmdbfetchjson(strtmdbapifullurl, f"f_tmdbcompanytosql({lngcompanyid})")
        if data is None:
            return
        
        #strapicompanyfordb = json.dumps(data, ensure_ascii=False)
        lngcompanystatuscode = 0
        if 'status_code' in data:
            lngcompanystatuscode = data['status_code']
        if lngcompanystatuscode <= 1:
            # API request result is not an error
            # Extract data using the keys
            strcompanydescription = ""
            if 'description' in data:
                strcompanydescription = data['description']
            strcompanylogopath = ""
            if 'logo_path' in data:
                strcompanylogopath = data['logo_path']
            strcompanyname = ""
            if 'name' in data:
                strcompanyname = data['name']
            strcompanyheadquarters = ""
            if 'headquarters' in data:
                strcompanyheadquarters = data['headquarters']
            strcompanyhomepage = ""
            if 'homepage' in data:
                strcompanyhomepage = data['homepage']
            strcompanyorigincountry = ""
            if 'origin_country' in data:
                strcompanyorigincountry = data['origin_country']
            lngcompanyparentid = 0
            if 'parent_company' in data:
                if data['parent_company']:
                    if 'id' in data['parent_company']:
                        lngcompanyparentid = data['parent_company']['id']
            
            if strcompanyhomepage: 
                if len(strcompanyhomepage) > 500:
                    # If homepage URL is too long, we chop it
                    strcompanyhomepage = strcompanyhomepage[:500]
            if strcompanyheadquarters: 
                if len(strcompanyheadquarters) > 200:
                    # If headquarters data is too long, we chop it
                    strcompanyheadquarters = strcompanyheadquarters[:200]
            
            # print(f"{strcompanyname}")
            
            arrcompanycouples = {}
            arrcompanycouples["ID_COMPANY"] = lngcompanyid
            #arrcompanycouples["API_URL"] = strtmdbapicompanyurl
            #arrcompanycouples["CRAWLER_VERSION"] = 1
            #arrcompanycouples["API_RESULT"] = strapicompanyfordb
            if strcompanydescription != "":
                arrcompanycouples["DESCRIPTION"] = strcompanydescription
            if strcompanylogopath != "":
                arrcompanycouples["LOGO_PATH"] = strcompanylogopath
            if strcompanyname != "":
                arrcompanycouples["NAME"] = strcompanyname
            if strcompanyheadquarters != "":
                arrcompanycouples["HEADQUARTERS"] = strcompanyheadquarters
            if strcompanyhomepage != "":
                arrcompanycouples["HOMEPAGE_URL"] = strcompanyhomepage
            if strcompanyorigincountry != "":
                arrcompanycouples["ORIGIN_COUNTRY"] = strcompanyorigincountry
            arrcompanycouples["ID_PARENT"] = lngcompanyparentid
            
            strsqltablename = "T_WC_TMDB_COMPANY"
            strsqlupdatecondition = f"ID_COMPANY = {lngcompanyid}"
            cp.f_sqlupdatearray(strsqltablename,arrcompanycouples,strsqlupdatecondition,1)

def f_tmdbcompanysetcreditscompleted(lngcompanyid):
    """
    Mark a company as fully processed by setting TIM_CREDITS_COMPLETED timestamp.

    Parameters:
    -----------
    lngcompanyid : int
        The TMDb company ID to update

    Returns:
    --------
    None
    """
    global paris_tz
    global connectioncp

    if lngcompanyid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_COMPANY"
        strsqlupdatecondition = f"ID_COMPANY = {lngcompanyid}"
        strtimcreditscompleted = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        strsqlupdatesetclause = f"TIM_CREDITS_COMPLETED = '{strtimcreditscompleted}', TIM_UPDATED = '{strtimcreditscompleted}'"
        strsqlupdate = f"UPDATE {strsqltablename} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdbcompanyimagestosql(lngcompanyid):
    """
    Fetch and store images for a company from TMDb API.

    Parameters:
    -----------
    lngcompanyid : int
        The TMDb company ID to fetch images for

    Returns:
    --------
    bool
        True if successful, False if failed
    """
    f_tmdbcontentimagesstosql(lngcompanyid, "company", "T_WC_TMDB_COMPANY", "T_WC_TMDB_COMPANY_IMAGE", "ID_COMPANY", "LOGO_PATH", "logo")

def f_tmdbcompanytosqleverything(lngcompanyid):
    """
    Fetch and store complete company data including details and images.

    Parameters:
    -----------
    lngcompanyid : int
        The TMDb company ID to fetch

    Returns:
    --------
    None
    """
    f_tmdbcompanytosql(lngcompanyid)
    f_tmdbcompanysetcreditscompleted(lngcompanyid)
    f_tmdbcompanyimagestosql(lngcompanyid)

# https://developer.themoviedb.org/reference/network-details

def f_tmdbnetworktosql(lngnetworkid):
    """
    Fetch network details from TMDb API and store in database.

    Parameters:
    -----------
    lngnetworkid : int
        The TMDb network ID to fetch

    Returns:
    --------
    None
    """
    global strtmdbapidomainurl
    global headers
    global strlanguagecountry
    global strlanguage

    if lngnetworkid > 0:
        strtmdbapinetworkurl = "3/network/" + str(lngnetworkid)
        strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapinetworkurl
        # print(strtmdbapifullurl)
        data = f_tmdbfetchjson(strtmdbapifullurl, f"f_tmdbnetworktosql({lngnetworkid})")
        if data is None:
            return
        
        #strapinetworkfordb = json.dumps(data, ensure_ascii=False)
        lngnetworkstatuscode = 0
        if 'status_code' in data:
            lngnetworkstatuscode = data['status_code']
        if lngnetworkstatuscode <= 1:
            # API request result is not an error
            # Extract data using the keys
            strnetworklogopath = ""
            if 'logo_path' in data:
                strnetworklogopath = data['logo_path']
            strnetworkname = ""
            if 'name' in data:
                strnetworkname = data['name']
            strnetworkheadquarters = ""
            if 'headquarters' in data:
                strnetworkheadquarters = data['headquarters']
            strnetworkhomepage = ""
            if 'homepage' in data:
                strnetworkhomepage = data['homepage']
            strnetworkorigincountry = ""
            if 'origin_country' in data:
                strnetworkorigincountry = data['origin_country']
            lngnetworkparentid = 0
            
            if strnetworkhomepage: 
                if len(strnetworkhomepage) > 500:
                    # If homepage URL is too long, we chop it
                    strnetworkhomepage = strnetworkhomepage[:500]
            
            # print(f"{strnetworkname}")
            
            arrnetworkcouples = {}
            arrnetworkcouples["ID_NETWORK"] = lngnetworkid
            #arrnetworkcouples["API_URL"] = strtmdbapinetworkurl
            #arrnetworkcouples["CRAWLER_VERSION"] = 1
            #arrnetworkcouples["API_RESULT"] = strapinetworkfordb
            if strnetworklogopath != "":
                arrnetworkcouples["LOGO_PATH"] = strnetworklogopath
            if strnetworkname != "":
                arrnetworkcouples["NAME"] = strnetworkname
            if strnetworkhomepage != "":
                arrnetworkcouples["HOMEPAGE_URL"] = strnetworkhomepage
            if strnetworkorigincountry != "":
                arrnetworkcouples["ORIGIN_COUNTRY"] = strnetworkorigincountry
            if strnetworkheadquarters != "":
                arrnetworkcouples["HEADQUARTERS"] = strnetworkheadquarters
            
            strsqltablename = "T_WC_TMDB_NETWORK"
            strsqlupdatecondition = f"ID_NETWORK = {lngnetworkid}"
            cp.f_sqlupdatearray(strsqltablename,arrnetworkcouples,strsqlupdatecondition,1)

def f_tmdbnetworksetcreditscompleted(lngnetworkid):
    """
    Mark a network as fully processed by setting TIM_CREDITS_COMPLETED timestamp.

    Parameters:
    -----------
    lngnetworkid : int
        The TMDb network ID to update

    Returns:
    --------
    None
    """
    global paris_tz
    global connectioncp

    if lngnetworkid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_NETWORK"
        strsqlupdatecondition = f"ID_NETWORK = {lngnetworkid}"
        strtimcreditscompleted = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        strsqlupdatesetclause = f"TIM_CREDITS_COMPLETED = '{strtimcreditscompleted}', TIM_UPDATED = '{strtimcreditscompleted}'"
        strsqlupdate = f"UPDATE {strsqltablename} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdbnetworkimagestosql(lngnetworkid):
    """
    Fetch and store images for a network from TMDb API.

    Parameters:
    -----------
    lngnetworkid : int
        The TMDb network ID to fetch images for

    Returns:
    --------
    bool
        True if successful, False if failed
    """
    f_tmdbcontentimagesstosql(lngnetworkid, "network", "T_WC_TMDB_NETWORK", "T_WC_TMDB_NETWORK_IMAGE", "ID_NETWORK", "LOGO_PATH", "logo")
    
def f_tmdbnetworktosqleverything(lngnetworkid):
    """
    Fetch and store complete network data including details and images.

    Parameters:
    -----------
    lngnetworkid : int
        The TMDb network ID to fetch

    Returns:
    --------
    None
    """
    f_tmdbnetworktosql(lngnetworkid)
    f_tmdbnetworksetcreditscompleted(lngnetworkid)
    f_tmdbnetworkimagestosql(lngnetworkid)

def f_tmdbkeywordtosql(lngkeywordid, strkeywordname):
    """
    Store a keyword in the database with its ID and name.

    Parameters:
    -----------
    lngkeywordid : int
        The TMDb keyword ID
    strkeywordname : str
        The keyword name

    Returns:
    --------
    None
    """
    global strtmdbapidomainurl
    global headers
    global strlanguagecountry
    global strlanguage
    
    if lngkeywordid > 0:
        arrkeywordcouples = {}
        arrkeywordcouples["ID_KEYWORD"] = lngkeywordid
        if strkeywordname != "":
            arrkeywordcouples["NAME"] = strkeywordname
        
        strsqltablename = "T_WC_TMDB_KEYWORD"
        strsqlupdatecondition = f"ID_KEYWORD = {lngkeywordid}"
        cp.f_sqlupdatearray(strsqltablename,arrkeywordcouples,strsqlupdatecondition,1)

def f_tmdbkeywordsetcreditscompleted(lngkeywordid):
    """
    Mark a keyword as fully processed by setting TIM_CREDITS_COMPLETED timestamp.

    Parameters:
    -----------
    lngkeywordid : int
        The TMDb keyword ID to update

    Returns:
    --------
    None
    """
    global paris_tz
    global connectioncp

    if lngkeywordid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_KEYWORD"
        strsqlupdatecondition = f"ID_KEYWORD = {lngkeywordid}"
        strtimcreditscompleted = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        strsqlupdatesetclause = f"TIM_CREDITS_COMPLETED = '{strtimcreditscompleted}', TIM_UPDATED = '{strtimcreditscompleted}'"
        strsqlupdate = f"UPDATE {strsqltablename} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdbkeywordtosqleverything(lngkeywordid, strkeywordname):
    """
    Store complete keyword data and mark as processed.

    Parameters:
    -----------
    lngkeywordid : int
        The TMDb keyword ID
    strkeywordname : str
        The keyword name

    Returns:
    --------
    None
    """
    f_tmdbkeywordtosql(lngkeywordid, strkeywordname)
    f_tmdbkeywordsetcreditscompleted(lngkeywordid)

# https://developer.themoviedb.org/reference/list-details

def f_tmdblisttosql(lnglistid):
    """
    Fetch list details and items from TMDb API and store in database.

    Parameters:
    -----------
    lnglistid : int
        The TMDb list ID to fetch

    Returns:
    --------
    None
    """
    global strtmdbapidomainurl
    global headers
    global strlanguagecountry
    global strlanguage
    
    if lnglistid > 0:
        lngpage = 1
        lngdisplayorder = 0
        lngtotalpages = 0
        strmovieidlist = ""
        strserieidlist = ""
        intencore = True
        while intencore:
            strtmdbapilisturl = "3/list/" + str(lnglistid) + "?language=" + strlanguagecountry + "&page=" + str(lngpage)
            strtmdbapifullurl = strtmdbapidomainurl + "/" + strtmdbapilisturl
            # print(strtmdbapifullurl)
            data = f_tmdbfetchjson(strtmdbapifullurl, f"f_tmdblisttosql({lnglistid}, page={lngpage})")
            if data is None:
                return
            #strapilistfordb = json.dumps(data, ensure_ascii=False)
            lngliststatuscode = 0
            if 'status_code' in data:
                lngliststatuscode = data['status_code']
            if lngliststatuscode <= 1:
                # API request result is not an error
                if lngpage == 1:
                    # Extract data using the keys
                    strlistdesc = ""
                    if 'description' in data:
                        strlistdesc = data['description']
                    strlistposterpath = ""
                    if 'poster_path' in data:
                        strlistposterpath = data['poster_path']
                    strlistname = ""
                    if 'name' in data:
                        strlistname = data['name']
                    strcreatedby = ""
                    if 'created_by' in data:
                        strcreatedby = data['created_by']
                    
                    # print(f"{strlistname}")
                    # print(f"{strlistposterpath}")
                    # print(f"Description: {strlistdesc}")
                    
                    arrlistcouples = {}
                    arrlistcouples["ID_LIST"] = lnglistid
                    #arrlistcouples["API_URL"] = strtmdbapilisturl
                    #arrlistcouples["CRAWLER_VERSION"] = 1
                    #arrlistcouples["API_RESULT"] = strapilistfordb
                    arrlistcouples["DESCRIPTION"] = strlistdesc
                    if strlistposterpath is not None and strlistposterpath != "":
                        arrlistcouples["POSTER_PATH"] = strlistposterpath
                    if strlistname != "":
                        arrlistcouples["NAME"] = strlistname
                    if strcreatedby != "":
                        arrlistcouples["CREATED_BY"] = strcreatedby
                    
                    strsqltablename = "T_WC_TMDB_LIST"
                    strsqlupdatecondition = f"ID_LIST = {lnglistid}"
                    cp.f_sqlupdatearray(strsqltablename,arrlistcouples,strsqlupdatecondition,1)
                results = data['items']
                lngtotalpages = data['total_pages']
                for row in results:
                    lngmovieid = row['id']
                    intadult = row['adult']
                    strmediatype = row['media_type'];
                    if strmediatype == "movie":
                        # It is a movie
                        lngdisplayorder += 1
                        if strmovieidlist != "":
                            strmovieidlist += ","
                        strmovieidlist += str(lngmovieid)
                        arrlistcouples = {}
                        arrlistcouples["ID_LIST"] = lnglistid
                        arrlistcouples["ID_MOVIE"] = lngmovieid
                        arrlistcouples["DISPLAY_ORDER"] = lngdisplayorder
                        strsqltablename = "T_WC_TMDB_MOVIE_LIST"
                        strsqlupdatecondition = f"ID_LIST = {lnglistid} AND ID_MOVIE = {lngmovieid}"
                        cp.f_sqlupdatearray(strsqltablename,arrlistcouples,strsqlupdatecondition,1)
                    else:
                        # It is a TV serie
                        lngdisplayorder += 1
                        if strserieidlist != "":
                            strserieidlist += ","
                        strserieidlist += str(lngmovieid)
                        arrlistcouples = {}
                        arrlistcouples["ID_LIST"] = lnglistid
                        arrlistcouples["ID_SERIE"] = lngmovieid
                        arrlistcouples["DISPLAY_ORDER"] = lngdisplayorder
                        strsqltablename = "T_WC_TMDB_SERIE_LIST"
                        strsqlupdatecondition = f"ID_LIST = {lnglistid} AND ID_SERIE = {lngmovieid}"
                        cp.f_sqlupdatearray(strsqltablename,arrlistcouples,strsqlupdatecondition,1)
            lngpage += 1
            if lngpage > lngtotalpages:
                intencore = False
        if strmovieidlist != "":
            strsqldelete = "DELETE FROM " + strsqlns + "TMDB_MOVIE_LIST WHERE ID_LIST = " + str(lnglistid) + " AND ID_MOVIE NOT IN (" + strmovieidlist + ") "
            cursor2 = connectioncp.cursor()
            cursor2.execute(strsqldelete)
            connectioncp.commit()
        if strserieidlist != "":
            strsqldelete = "DELETE FROM " + strsqlns + "TMDB_SERIE_LIST WHERE ID_LIST = " + str(lnglistid) + " AND ID_SERIE NOT IN (" + strserieidlist + ") "
            cursor2 = connectioncp.cursor()
            cursor2.execute(strsqldelete)
            connectioncp.commit()
        try:
            print(f"DEBUG: list-poster: start (ID_LIST={lnglistid})")
            if 'strlistposterpath' in locals():
                print(f"DEBUG: list-poster: strlistposterpath exists (value={strlistposterpath!r})")
                cursor2 = connectioncp.cursor()
                cursor2.execute(f"SELECT POSTER_PATH FROM T_WC_TMDB_LIST WHERE ID_LIST = {lnglistid}")
                row = cursor2.fetchone()
                db_poster = None
                if row is not None and 'POSTER_PATH' in row:
                    db_poster = row['POSTER_PATH']
                print(f"DEBUG: list-poster: db_poster={db_poster!r} (row_is_none={row is None})")
                need_poster = (strlistposterpath is None or strlistposterpath == "") and (db_poster is None or db_poster == "")
                print(f"DEBUG: list-poster: need_poster={need_poster}")
                if need_poster:
                    print("DEBUG: list-poster: selecting best movie poster by IMDb rating")
                    strsql = ""
                    strsql += "SELECT m.POSTER_PATH AS poster_path, r.averageRating AS rating "
                    strsql += "FROM T_WC_TMDB_MOVIE m "
                    strsql += "INNER JOIN T_WC_TMDB_MOVIE_LIST ml ON ml.ID_MOVIE = m.ID_MOVIE "
                    strsql += "INNER JOIN T_WC_IMDB_MOVIE_RATING_IMPORT r ON r.tconst = m.ID_IMDB "
                    strsql += f"WHERE ml.ID_LIST = {lnglistid} AND r.averageRating IS NOT NULL AND m.POSTER_PATH IS NOT NULL "
                    strsql += "ORDER BY r.averageRating DESC "
                    strsql += "LIMIT 1 "
                    cursor2.execute(strsql)
                    row = cursor2.fetchone()
                    if row is not None and 'poster_path' in row and row['poster_path'] is not None and row['poster_path'] != "":
                        print(f"DEBUG: list-poster: candidate poster found (poster_path={row['poster_path']!r})")
                        arrlistcouples = {}
                        arrlistcouples["POSTER_PATH"] = row['poster_path']
                        strsqltablename = "T_WC_TMDB_LIST"
                        strsqlupdatecondition = f"ID_LIST = {lnglistid}"
                        cp.f_sqlupdatearray(strsqltablename, arrlistcouples, strsqlupdatecondition, 1)
                        print("DEBUG: list-poster: poster updated in T_WC_TMDB_LIST")
                    else:
                        print(f"DEBUG: list-poster: no candidate poster found (row={row!r})")
                else:
                    print("DEBUG: list-poster: skipping (poster already present in TMDb payload or DB)")
            else:
                print("DEBUG: list-poster: skipping (strlistposterpath not defined in locals)")
        except Exception as e:
            print(f"Warning: failed to set list poster from highest IMDb-rated movie: {e}")

def f_tmdblistsetcreditscompleted(lnglistid):
    """
    Mark a list as fully processed by setting TIM_CREDITS_COMPLETED timestamp.

    Parameters:
    -----------
    lnglistid : int
        The TMDb list ID to update

    Returns:
    --------
    None
    """
    global paris_tz
    global connectioncp

    if lnglistid > 0:
        cursor2 = connectioncp.cursor()
        strsqltablename = "T_WC_TMDB_LIST"
        strsqlupdatecondition = f"ID_LIST = {lnglistid}"
        strtimcreditscompleted = datetime.now(paris_tz).strftime("%Y-%m-%d %H:%M:%S")
        strsqlupdatesetclause = f"TIM_CREDITS_COMPLETED = '{strtimcreditscompleted}', TIM_UPDATED = '{strtimcreditscompleted}'"
        strsqlupdate = f"UPDATE {strsqltablename} SET {strsqlupdatesetclause} WHERE {strsqlupdatecondition};"
        cursor2.execute(strsqlupdate)
        connectioncp.commit()

def f_tmdblisttosqleverything(lnglistid):
    """
    Fetch and store complete list data including all movies and series.

    Parameters:
    -----------
    lnglistid : int
        The TMDb list ID to fetch

    Returns:
    --------
    None
    """
    f_tmdblisttosql(lnglistid)
    f_tmdblistsetcreditscompleted(lnglistid)

def f_genrestranslatefr(strmoviegenres):
    """
    Translate genre names from English to French in a pipe-delimited string.

    Parameters:
    -----------
    strmoviegenres : str
        A pipe-delimited string of genre names (e.g., '|Action|Comedy|Drama|')

    Returns:
    --------
    str
        The translated pipe-delimited string with French genre names
    """
    strmoviegenres = strmoviegenres.replace("|Action|","|Action|")
    strmoviegenres = strmoviegenres.replace("|Adventure|","|Aventure|")
    strmoviegenres = strmoviegenres.replace("|Animation|","|Animation|")
    strmoviegenres = strmoviegenres.replace("|Comedy|","|Comédie|")
    strmoviegenres = strmoviegenres.replace("|Crime|","|Crime|")
    strmoviegenres = strmoviegenres.replace("|Documentary|","|Documentaire|")
    strmoviegenres = strmoviegenres.replace("|Drama|","|Drame|")
    strmoviegenres = strmoviegenres.replace("|Family|","|Familial|")
    strmoviegenres = strmoviegenres.replace("|Fantasy|","|Fantastique|")
    strmoviegenres = strmoviegenres.replace("|History|","|Histoire|")
    strmoviegenres = strmoviegenres.replace("|Horror|","|Horreur|")
    strmoviegenres = strmoviegenres.replace("|Music|","|Musique|")
    strmoviegenres = strmoviegenres.replace("|Mystery|","|Mystère|")
    strmoviegenres = strmoviegenres.replace("|Romance|","|Romance|")
    strmoviegenres = strmoviegenres.replace("|Science Fiction|","|Science-Fiction|")
    strmoviegenres = strmoviegenres.replace("|Thriller|","|Thriller|")
    strmoviegenres = strmoviegenres.replace("|TV Movie|","|Téléfilm|")
    strmoviegenres = strmoviegenres.replace("|War|","|Guerre|")
    strmoviegenres = strmoviegenres.replace("|Western|","|Western|")
    return strmoviegenres

