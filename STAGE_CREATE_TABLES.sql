DROP TABLE IF EXISTS STAGE.MOVIE_RELEASES
CREATE TABLE STAGE.MOVIE_RELEASES
(
DT_PERIODO VARCHAR(8000),
ID VARCHAR(8000),
TITLE VARCHAR(8000)
)

DROP TABLE IF EXISTS STAGE.MOVIE_DETAIL
CREATE TABLE STAGE.MOVIE_DETAIL
(
ID VARCHAR(8000),
MOVIE_TITLE VARCHAR(8000),
ORI_LANGUAGE VARCHAR(8000),
ORI_TITLE VARCHAR(8000),
POPULARITY VARCHAR(8000),
STATUS  VARCHAR(8000),
RUNTIME VARCHAR(8000),
REVENUE VARCHAR(8000),
BUGDET VARCHAR(8000),
VOTE_AVG VARCHAR(8000),
VOTE_CNT VARCHAR(8000)
)

DROP TABLE IF EXISTS STAGE.MOVIE_DETAIL_PRODUCTION
CREATE TABLE STAGE.MOVIE_DETAIL_PRODUCTION
(
ID VARCHAR(8000),
PRODUCTION_COMPANIES_ID VARCHAR(8000),
PRODUCTION_COMPANIES_NAME VARCHAR(8000),
PRODUCTION_COMPANIES_ORIGIN_COUNTRY VARCHAR(8000)
)


DROP TABLE IF EXISTS STAGE.MOVIE_DETAIL_GENRE
CREATE TABLE STAGE.MOVIE_DETAIL_GENRE
(
ID VARCHAR(8000),
GEN.GENRE_ID VARCHAR(8000), 
GEN.GENRE_NAME VARCHAR(8000)
)

