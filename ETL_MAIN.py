import Extract_Release_Year_JSON as Extract
import Data
server = Data.server
database = Data.database
username = Data.username
password = Data.password



def main():
    year = 2019
    home = "G:/PROJECTS/GIT/TMDB_ETL_PY"
    limit = 1
    Extract.startExtractionSQL(year, home, limit, server, database, username, password)


if __name__== "__main__":
    main()

