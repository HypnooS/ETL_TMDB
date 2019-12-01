import pyodbc

def processDataETLStage(server, database, username, password):
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};', server=server, database=database, UID=username,
                      PWD=password)
    cursor = conn.cursor()
    print("Executing Step 2.1 - STAGING DATA...")
    cursor.execute("EXEC STAGE.INSERT_MOVIES_RELEASES")
    cursor.execute("EXEC STAGE.INSERT_PERIOD")
    cursor.commit()
    cursor.close()
    conn.close()
    print("Executing Step 2.1 - STAGING DATA... COMMITTED....")

def processDataETLDW(server, database, username, password):
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};', server=server, database=database, UID=username,
                      PWD=password)
    cursor = conn.cursor()
    print("Executing Step 2.1 - LOAD DIMS...")
    cursor.execute("EXEC MOVIE.SCD_DIM_PERIOD")
    cursor.execute("EXEC MOVIE.SCD_DIM_MOVIE_NAME")
    cursor.execute("EXEC MOVIE.SCD_DIM_MOVIE_PRODUCTION")
    cursor.execute("EXEC MOVIE.SDC_DIM_MOVIE_GENRE")
    cursor.execute("EXEC MOVIE.SCD_DIM_MOVIE_COUNTRY")
    cursor.execute("EXEC MOVIE.SCD_DIM_MOVIE_STATUS")
    print("Executing Step 2.2 - LOADING FACT...")
    cursor.execute("EXEC MOVIE.INSERT_FACT")
    cursor.commit()
    cursor.close()
    conn.close()
    print("Executing Step 2.1 - DATA WAREHOUSING... COMMITTED....")