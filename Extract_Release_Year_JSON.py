from pandas.io.json import json_normalize
import pandas as pd
import tmdbsimple as tmdb
import http.client
import json
import pyodbc

pd.set_option('display.max_columns', None)
pd.set_option('display.max_row', None)

api_key = '239cc23f637b9bd3ca82377ff1abc2bf'
tmdb.API_KEY = api_key
language = 'en-US'
region = 'US'
home = "G:\PROJECTS\GIT\TMDB_ETL_PY"

def getRelease(api_key, language, region):
    conn = http.client.HTTPSConnection("api.themoviedb.org")
    conn.request("GET", "/3/movie/now_playing?api_key=" + api_key + "&region=" + region + "&language=" + language, "{}")
    json_d = json.loads(conn.getresponse().read())
    df_release = pd.DataFrame(json_normalize(data=json_d['results']))
    total_pages = json_d['total_pages']
    page = json_d['page'] + 1
    print("INFO: " + str(page) + " pages of " + str(total_pages))
    while total_pages > page:
        conn = http.client.HTTPSConnection("api.themoviedb.org")
        conn.request("GET", "/3/movie/now_playing?api_key=" + api_key + "&region=" + region + "&language=" + language, "{}")
        json_d = json.loads(conn.getresponse().read())
        df_release = df_release.append(pd.DataFrame(json_normalize(data=json_d['results'])), ignore_index=True)
        page = page + 1
        print("INFO: " + str(page) + " pages of " + str(total_pages))
    return df_release

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
    return df_movies

def start_extraction_csv(year_iter, home):
    print("The year is " + str(year_iter))
    df_year = getReleaseYearLimited(api_key, year_iter, language, region, 50)
    df_movie_p = getMovieDetailProduction(api_key, df_year, language, region)
    df_movie_g = getMovieDetailGenres(api_key, df_year, language, region)
    nm_file_year = home + '/DATA/CSV/movie_releases_' + str(year_iter) + '.csv'
    nm_file_movie_p = home + '/DATA/CSV/movie_production_' + str(year_iter) + '.csv'
    nm_file_movie_g = home + '/DATA/CSV/movie_genres_' + str(year_iter) + '.csv'
    df_year.to_csv(nm_file_year, sep=';', index=False)
    df_movie_p.to_csv(nm_file_movie_p, sep=';', index=False)
    df_movie_g.to_csv(nm_file_movie_g, sep=';', index=False)

def start_extraction_sql(year_iter, home):
    print("The year is " + str(year_iter))
    df_year = getReleaseYearLimited(api_key, year_iter, language, region, 1)
    df_movie_p = getMovieDetailProduction(api_key, df_year, language, region)
    df_movie_g = getMovieDetailGenres(api_key, df_year, language, region)
    nm_file_year = home + '/DATA/CSV/movie_releases_' + str(year_iter) + '.csv'
    nm_file_movie_p = home + '/DATA/CSV/movie_production_' + str(year_iter) + '.csv'
    nm_file_movie_g = home + '/DATA/CSV/movie_genres_' + str(year_iter) + '.csv'


print("BURNING TO THE HELL")
year_iter = input('WHICH YEAR???')
start_extraction_sql(year_iter, home)
