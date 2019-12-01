from pandas.io.json import json_normalize
import pandas as pd
import http.client
import json
import pyodbc
import Data

pd.set_option('display.max_columns', None)
pd.set_option('display.max_row', None)

api_key = Data.api_key
language = 'en-US'
region = 'US'
home = Data.home


def ifnull(var, ret):
  if var is None:
    return ret
  return var

def getUpComing(api_key, language, region):
    conn = http.client.HTTPSConnection("api.themoviedb.org")
    conn.request("GET", "/3/movie/now_playing?api_key=" + api_key + "&region=" + region + "&language=" + language, "{}")
    json_d = json.loads(conn.getresponse().read())
    df_release_u = pd.DataFrame(json_normalize(data=json_d['results']))
    total_pages = json_d['total_pages']
    page = json_d['page'] + 1
    print("INFO: " + str(page) + " pages of " + str(total_pages))
    while total_pages > page:
        conn = http.client.HTTPSConnection("api.themoviedb.org")
        conn.request("GET", "/3/movie/upcoming?api_key=" + api_key + "&region=" + region + "&language=" + language, "{}")
        json_d = json.loads(conn.getresponse().read())
        df_release_u = df_release_u.append(pd.DataFrame(json_normalize(data=json_d['results'])), ignore_index=True)
        page = page + 1
        print("INFO: " + str(page) + " pages of " + str(total_pages))
    print("INFO: MOVIE UP COMING EXTRACTED")
    return df_release_u

def getPlayingNow(api_key, language, region):
    conn = http.client.HTTPSConnection("api.themoviedb.org")
    conn.request("GET", "/3/movie/now_playing?api_key=" + api_key + "&region=" + region + "&language=" + language, "{}")
    json_d = json.loads(conn.getresponse().read())
    df_release_n = pd.DataFrame(json_normalize(data=json_d['results']))
    total_pages = json_d['total_pages']
    page = json_d['page'] + 1
    print("INFO: " + str(page) + " pages of " + str(total_pages))
    while total_pages > page:
        conn = http.client.HTTPSConnection("api.themoviedb.org")
        conn.request("GET", "/3/movie/now_playing?api_key=" + api_key + "&region=" + region + "&language=" + language, "{}")
        json_d = json.loads(conn.getresponse().read())
        df_release_n = df_release_n.append(pd.DataFrame(json_normalize(data=json_d['results'])), ignore_index=True)
        page = page + 1
        print("INFO: " + str(page) + " pages of " + str(total_pages))
    print("INFO: MOVIE PLAYING NOW EXTRACTED")
    return df_release_n

def getReleaseYear(api_key, year, language, region):
    conn = http.client.HTTPSConnection("api.themoviedb.org")
    conn.request("GET", "/3/discover/movie?api_key=" + api_key + "&primary_release_year=" + str(year) + "&region=" + region + "&language=" + language, "{}")
    res = conn.getresponse()
    json_d = json.loads(res.read())
    df_release = pd.DataFrame(json_normalize(data=json_d['results']))
    total_pages = json_d['total_pages']
    page = json_d['page'] + 1
    while total_pages > page:
        conn = http.client.HTTPSConnection("api.themoviedb.org")
        conn.request("GET", "/3/discover/movie?api_key=" + api_key + "&primary_release_year=" + str(
            year) + "&region=" + region + "&language=" + language + "&page=" + str(page), "{}")
        json_d = json.loads(conn.getresponse().read())
        df_release = df_release.append(pd.DataFrame(json_normalize(data=json_d['results'])), ignore_index=True)
        page = page + 1
        print("INFO: " + str(page) + " pages processed of " + str(total_pages))
    print("INFO: MOVIE RELEASE FULL EXTRACTED")
    return df_release

def getReleaseYearLimited(api_key, year, language, region, limit):
    conn = http.client.HTTPSConnection("api.themoviedb.org")
    conn.request("GET", "/3/discover/movie?api_key=" + api_key + "&primary_release_year=" + str(year) + "&region=" + region + "&language=" + language, "{}")
    res = conn.getresponse()
    json_d = json.loads(res.read())
    df_release = pd.DataFrame(json_normalize(data=json_d['results']))
    page = json_d['page'] + 1
    while limit > page:
        conn = http.client.HTTPSConnection("api.themoviedb.org")
        conn.request("GET", "/3/discover/movie?api_key=" + api_key + "&primary_release_year=" + str(
            year) + "&region=" + region + "&language=" + language + "&page=" + str(page), "{}")
        json_d = json.loads(conn.getresponse().read())
        df_release = df_release.append(pd.DataFrame(json_normalize(data=json_d['results'])), ignore_index=True)
        page = page + 1
        print("INFO: " + str(page) + " pages of the limit " + str(limit))
    print("INFO: MOVIE RELEASE LIMITED EXTRACTED")
    return df_release


def getMovieDetailGenres(api_key, dataframe, language, region):
    listIDs = dataframe['id'].to_list()
    conn = http.client.HTTPSConnection("api.themoviedb.org")
    conn.request("GET", "/3/movie/"+str(listIDs.pop(0))+"?api_key=" + api_key + "&language=" + language + "&region=" + region, "{}")
    json_d = json.loads(conn.getresponse().read())
    df_movies = pd.DataFrame(json_normalize(data=json_d,
                                            record_path=['genres'],
                                            record_prefix='genre_',
                                            meta=['adult','budget','id','original_title','original_language','popularity',
                                                  'revenue','runtime','status','vote_average', 'vote_count']))
    while len(listIDs) > 0:
        id = listIDs.pop(0)
        conn = http.client.HTTPSConnection("api.themoviedb.org")
        conn.request("GET",
                     "/3/movie/" + str(id) + "?api_key=" + api_key + "&language=" + language + "&region=" + region,
                     "{}")
        json_d = json.loads(conn.getresponse().read())
        try:
            df_movies = df_movies.append(pd.DataFrame(json_normalize(data=json_d,
                                                                     record_path=['genres'],
                                                                     record_prefix='genre_',
                                                                     meta=['adult','budget','id','original_title','original_language','popularity',
                                                                           'revenue','runtime','status','vote_average', 'vote_count'])))
            print("INFO: Only left:" + str(len(listIDs)))
        except KeyError:
            print("INFO: ID does not exists:" + str(id))
    print("INFO: MOVIE GENRE EXTRACTED")
    return df_movies

def getMovieDetailCountry(api_key, dataframe, language, region):
    listIDs = dataframe['id'].to_list()
    conn = http.client.HTTPSConnection("api.themoviedb.org")
    conn.request("GET", "/3/movie/"+str(listIDs.pop(0))+"?api_key=" + api_key + "&language=" + language + "&region=" + region, "{}")
    json_d = json.loads(conn.getresponse().read())
    df_movies = pd.DataFrame(json_normalize(data=json_d,
                                            record_path=['production_countries'],
                                            record_prefix='production_countries_',
                                            meta=['adult','budget','id','original_title','original_language','popularity',
                                             'revenue','runtime','status','vote_average', 'vote_count']))
    while len(listIDs) > 0:
        id = listIDs.pop(0)
        conn = http.client.HTTPSConnection("api.themoviedb.org")
        conn.request("GET",
                     "/3/movie/" + str(id) + "?api_key=" + api_key + "&language=" + language + "&region=" + region,
                     "{}")
        json_d = json.loads(conn.getresponse().read())
        try:
            df_movies = df_movies.append(pd.DataFrame(json_normalize(data=json_d,
                                                                     record_path=['production_countries'],
                                                                     record_prefix='production_countries_',
                                                                     meta=['adult','budget','id','original_title','original_language','popularity',
                                                                           'revenue','runtime','status','vote_average', 'vote_count'])))
            print("INFO: Only left:" + str(len(listIDs)))
        except KeyError:
            print("INFO: ID does not exists:" + str(id))
    print("INFO: MOVIE COUNTRY EXTRACTED")
    return df_movies

def getMovieDetailProduction(api_key, dataframe, language, region):
    listIDs = dataframe['id'].to_list()
    conn = http.client.HTTPSConnection("api.themoviedb.org")
    conn.request("GET", "/3/movie/"+str(listIDs.pop(0))+"?api_key=" + api_key + "&language=" + language + "&region=" + region, "{}")
    json_d = json.loads(conn.getresponse().read())
    df_movies = pd.DataFrame(json_normalize(data=json_d,
                                                                 record_path=['production_companies'],
                                                                 record_prefix='production_companies_',
                                                                 meta=['adult','budget','id','original_title','original_language','popularity',
                                                                       'revenue','runtime','status','vote_average', 'vote_count']))
    while len(listIDs) > 0:
        id = listIDs.pop(0)
        conn = http.client.HTTPSConnection("api.themoviedb.org")
        conn.request("GET",
                     "/3/movie/" + str(id) + "?api_key=" + api_key + "&language=" + language + "&region=" + region,
                     "{}")
        json_d = json.loads(conn.getresponse().read())
        try:
            df_movies = df_movies.append(pd.DataFrame(json_normalize(data=json_d,
                                                                     record_path=['production_companies'],
                                                                     record_prefix='production_companies_',
                                                                     meta=['adult','budget','id','original_title','original_language','popularity',
                                                                           'revenue','runtime','status','vote_average', 'vote_count'])))
            print("INFO: Only left:" + str(len(listIDs)))
        except KeyError:
            print("INFO: ID does not exists:" + str(id))
    print("INFO: MOVIE PRODUCTION EXTRACTED")
    return df_movies


def startExtractionSQL(year_iter, home, limit, server, database, username, password):
    print("The year is " + str(year_iter))
    df_nowplaying = getPlayingNow(api_key, language, region)
    df_upcoming = getUpComing(api_key, language, region)
    df_year = getReleaseYearLimited(api_key, year_iter, language, region, limit)
    df_movie_p = getMovieDetailProduction(api_key, df_year, language, region)
    df_movie_c = getMovieDetailCountry(api_key, df_year, language, region)
    df_movie_g = getMovieDetailGenres(api_key, df_year, language, region)
    nm_file_now = home + '/DATA/CSV/movie_now_' + str(year_iter) + '.csv'
    nm_file_up = home + '/DATA/CSV/movie_coming_' + str(year_iter) + '.csv'
    nm_file_year = home + '/DATA/CSV/movie_releases_' + str(year_iter) + '.csv'
    nm_file_movie_p = home + '/DATA/CSV/movie_production_' + str(year_iter) + '.csv'
    nm_file_movie_c = home + '/DATA/CSV/movie_country_' + str(year_iter) + '.csv'
    nm_file_movie_g = home + '/DATA/CSV/movie_genres_' + str(year_iter) + '.csv'
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};', server=server, database=database, UID=username,
                          PWD=password)
    cursor = conn.cursor()
    print("Executing Step 1.1 - LOAD LANDINGS FOR ETL...")
    sendDataSQLMovieNowPlaying(conn, cursor, df_nowplaying)
    sendDataSQLMovieUpComing(conn, cursor, df_upcoming)
    sendDataSQLMovieRelease(conn, cursor, df_year)
    sendDataSQLMovieDetail(conn, cursor, df_movie_p)
    sendDataSQLMovieCountry(conn, cursor, df_movie_c)
    sendDataSQLMovieProduction(conn, cursor, df_movie_p)
    sendDataSQLMovieGenre(conn, cursor, df_movie_g)
    df_year.to_csv(nm_file_year, sep=';', index=False)
    df_movie_p.to_csv(nm_file_movie_p, sep=';', index=False)
    df_movie_g.to_csv(nm_file_movie_g, sep=';', index=False)
    df_nowplaying.to_csv(nm_file_now, sep=';', index=False)
    df_upcoming.to_csv(nm_file_up, sep=';', index=False)
    df_movie_p.to_csv(nm_file_movie_c, sep=";", index=False)

def sendDataSQLMovieGenre(conn, cursor, df_movie_g):
    conn.execute("TRUNCATE TABLE STAGE.STAGE.MOVIE_DETAIL_GENRE")
    for index, row in df_movie_g.iterrows():
        try:
            cursor.execute("INSERT INTO STAGE.STAGE.MOVIE_DETAIL_GENRE (ID,GENRE_ID,GENRE_NAME) SELECT ?,?,?"
                           , str(ifnull(row['id'], 'N/I'))
                           , str(ifnull(row['genre_id'], 'N/I'))
                           , str(ifnull(row['genre_name'], 'N/I'))
                           )
            ##print("INFO MOVIE_DETAIL_GENRE : Line Loaded (ROW "+ str(row) +") = "  + str(row['id']) + ' - ' + str(row['genre_name']))
        except pyodbc.ProgrammingError as ex:
            print("ERR MOVIE_DETAIL_GENRE: Line NOT LOADED (ROW " + str(row) + ") = " + str(
                row['id']) + ' - ' + str(row['genre_name']))
            print(ex.args[1])
    print("INFO: MOVIE DETAIL GENRE COMMITTED")
    conn.commit()


def sendDataSQLMovieProduction(conn, cursor, df_movie_p):
    conn.execute("TRUNCATE TABLE STAGE.STAGE.MOVIE_DETAIL_PRODUCTION")
    for index, row in df_movie_p.iterrows():
        try:
            cursor.execute(
                "INSERT INTO STAGE.STAGE.MOVIE_DETAIL_PRODUCTION (ID,PRODUCTION_COMPANIES_ID,PRODUCTION_COMPANIES_NAME,PRODUCTION_COMPANIES_ORIGIN_COUNTRY) SELECT ?,?,?,?"
                , str(ifnull(row['id'], 'N/I'))
                , str(ifnull(row['production_companies_id'], 'N/I'))
                , str(ifnull(row['production_companies_name'], 'N/I'))
                , str(ifnull(row['production_companies_origin_country'], 'N/I'))
                )
            ##print("INFO MOVIE_DETAIL_PRODUCTION : Line Loaded (ROW "+ str(row) +") = "  + str(row['id']) + ' - ' + str(row['production_companies_name']))
        except pyodbc.ProgrammingError as ex:
            print("ERR MOVIE_DETAIL_PRODUCTION: Line NOT LOADED (ROW " + str(row) + ") = " + str(
                row['id']) + ' - ' + str(row['production_companies_name']))
            print(ex.args[1])
    print("INFO: MOVIE DETAIL PRODUCTION COMMITTED")
    conn.commit()

def sendDataSQLMovieCountry(conn, cursor, df_movie_p):
    conn.execute("TRUNCATE TABLE STAGE.STAGE.MOVIE_DETAIL_COUNTRY")
    for index, row in df_movie_p.iterrows():
        try:
            cursor.execute(
                "INSERT INTO STAGE.STAGE.MOVIE_DETAIL_COUNTRY (ID,ISO_3166_1,NAME) SELECT ?,?,?"
                , str(ifnull(row['id'], 'N/I'))
                , str(ifnull(row['production_countries_iso_3166_1'], 'N/I'))
                , str(ifnull(row['production_countries_name'], 'N/I'))
                )
            ##print("INFO MOVIE_DETAIL_PRODUCTION : Line Loaded (ROW "+ str(row) +") = "  + str(row['id']) + ' - ' + str(row['production_companies_name']))
        except pyodbc.ProgrammingError as ex:
            print("ERR MOVIE_DETAIL_PRODUCTION: Line NOT LOADED (ROW " + str(row) + ") = " + str(
                row['id']) + ' - ' + str(row['production_companies_name']))
            print(ex.args[1])
    print("INFO: MOVIE DETAIL PRODUCTION COMMITTED")
    conn.commit()

def sendDataSQLMovieRelease(conn, cursor, df_year):
    conn.execute("TRUNCATE TABLE STAGE.STAGE.MOVIE_RELEASES")
    for index, row in df_year.iterrows():
        try:
            cursor.execute("INSERT INTO STAGE.STAGE.MOVIE_RELEASES (DT_PERIODO,ID,TITLE) SELECT ?,?,?"
                           , ifnull(str(row['release_date']), 'N/I')
                           , ifnull(str(row['id']), 'N/I')
                           , ifnull(str(row['title']), 'N/I')
                           )
            ##print("INFO MOVIE_RELEASES: Line Loaded (ROW "+ str(row) +") = "   + str(row['id']) + ' - ' + str(row['title']))
        except pyodbc.ProgrammingError as ex:
            print("ERR MOVIE_DETAIL: Line NOT LOADED (ROW " + str(row) + ") = " + str(
                row['id']) + ' - ' + str(row['title']))
    print("INFO: MOVIE DETAIL RELEASES COMMITTED")
    conn.commit()

def sendDataSQLMovieNowPlaying(conn, cursor, df_year):
    conn.execute("TRUNCATE TABLE STAGE.STAGE.MOVIE_NOW_PLAYING")
    for index, row in df_year.iterrows():
        try:
            cursor.execute("INSERT INTO STAGE.STAGE.MOVIE_NOW_PLAYING (DT_PERIODO,ID,TITLE) SELECT ?,?,?"
                           , ifnull(str(row['release_date']), 'N/I')
                           , ifnull(str(row['id']), 'N/I')
                           , ifnull(str(row['title']), 'N/I')
                           )
            ##print("INFO MOVIE_RELEASES: Line Loaded (ROW "+ str(row) +") = "   + str(row['id']) + ' - ' + str(row['title']))
        except pyodbc.ProgrammingError as ex:
            print("ERR MOVIE_NOW_PLAYING: Line NOT LOADED (ROW " + str(row) + ") = " + str(
                row['id']) + ' - ' + str(row['title']))
    print("INFO: MOVIE DETAIL NOW PLAYING COMMITTED")
    conn.commit()

def sendDataSQLMovieUpComing(conn, cursor, df_year):
    conn.execute("TRUNCATE TABLE STAGE.STAGE.MOVIE_UP_COMING")
    for index, row in df_year.iterrows():
        try:
            cursor.execute("INSERT INTO STAGE.STAGE.MOVIE_UP_COMING (DT_PERIODO,ID,TITLE) SELECT ?,?,?"
                           , ifnull(str(row['release_date']), 'N/I')
                           , ifnull(str(row['id']), 'N/I')
                           , ifnull(str(row['title']), 'N/I')
                           )
            ##print("INFO MOVIE_RELEASES: Line Loaded (ROW "+ str(row) +") = "   + str(row['id']) + ' - ' + str(row['title']))
        except pyodbc.ProgrammingError as ex:
            print("ERR MOVIE_UP_COMING: Line NOT LOADED (ROW " + str(row) + ") = " + str(
                row['id']) + ' - ' + str(row['title']))
    print("INFO: MOVIE UP COMING COMMITTED")
    conn.commit()


def sendDataSQLMovieDetail(conn, cursor, df_movie_p):
    conn.execute("TRUNCATE TABLE STAGE.STAGE.MOVIE_DETAIL")
    for index, row in df_movie_p.iterrows():
        try:
            cursor.execute(
                "INSERT INTO STAGE.STAGE.MOVIE_DETAIL (ID,ORI_LANGUAGE,ORI_TITLE,POPULARITY,STATUS,RUNTIME,REVENUE,BUGDET,VOTE_AVG,VOTE_CNT) SELECT ?,?,?,?,?,?,?,?,?,?"
                , str(ifnull(row['id'], 'N/I'))
                , str(ifnull(row['original_language'], 'N/I'))
                , str(ifnull(row['original_title'], 'N/I'))
                , str(ifnull(row['popularity'], 'N/I'))
                , str(ifnull(row['status'], 'N/I'))
                , str(ifnull(row['runtime'], 'N/I'))
                , str(ifnull(row['revenue'], 'N/I'))
                , str(ifnull(row['budget'], 'N/I'))
                , str(ifnull(row['vote_average'], 'N/I'))
                , str(ifnull(row['vote_count'], 'N/I'))
                )
            ##print("INFO MOVIE_DETAIL: Line Loaded (ROW "+ str(row) +") = "   + str(row['original_language']) + ' - ' + str(row['original_title']))
        except pyodbc.ProgrammingError as ex:
            print("ERR MOVIE_DETAIL: Line NOT LOADED (ROW " + str(row) + ") = " + str(
                row['id']) + ' - ' + str(row['original_title']))
            print(ex.args[1])
    print("INFO: MOVIE DETAIL COMMITTED")
    conn.commit()
