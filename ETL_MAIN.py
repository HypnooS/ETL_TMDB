import Extract_Release_Year_JSON as Extract
import TransformData as Transform
import Data
server = Data.server
database = Data.database
username = Data.username
password = Data.password
dw = 'DW'



def main():
    home = "G:/PROJECTS/GIT/TMDB_ETL_PY"
    limit = 100
    for year in range(2015, 2019):
        Extract.startExtractionSQL(year, home, limit, server, database, username, password)
        Transform.processDataETLStage(server, database, username, password)
        Transform.processDataETLDW(server, dw, username, password)

if __name__== "__main__":
    main()

