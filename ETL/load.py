from ETL.query import *
import pyodbc
import pandas as pd
from loggingHelper import logger

driver = 'Your SQL Driver here'
server = 'Your server name here'
db1 = 'Your database name'
tcon = 'yes'
uname = 'Your username'
pword = 'Your password'



def OpenDatabaseConnection():
    """
        purpose : This method is responsible for establishing connection with the database
        args : no args
        return : N/A
    """
    global cursor, conn
    try:
        logger.info("Attempting Database Connection...")
        conn = pyodbc.connect(Driver=driver, Server=server,Database=db1,Trusted_Connection=tcon,user=uname, password=pword)
        print('Connection')
        cursor = conn.cursor()
        logger.info("Database Connection established!!")
    except Exception as ex:
        logger.error("Database Connection failed. Please refer to the log for more details!")
        raise
    

def CloseDatabaseConnection():
    """
        purpose : This method is responsible for closing the database connection
        args : no args
        return : N/A
    """
    global cursor, conn
    cursor.close()
    conn.close()
    logger.info("Database connection closed!")

def DeleteData():
    """
        purpose : This method is responsible for deleting all the initial data from the database
             upon starting the ETL process.
        args : no args
        return : N/A
    """
    global cursor, conn
    try:
        logger.info("Deleting data from all the tables...")
        cursor.execute(genreTableDeleteQuery)
        cursor.execute(starsTableDeleteQuery)
        cursor.execute(genreMovieRelTableDeleteQuery)
        cursor.execute(starsMovieRelTableDeleteQuery)
        cursor.execute(moviesTableDeleteQuery)
        conn.commit()
        logger.info("Data deletion complete!!")
    except Exception as ex:
        logger.error("An error occured while deleting data. Please refer to the log.")
        raise
    

def InsertMasterTableData(mastertableData, masterData):
    """
        purpose : This method is responsible for inserting data into the master table ; star &
            genre
        args : mastertableData -> string containing the table name
                masterData -> a dictionary containing the master data
        return : N/A
    """
    global cursor, conn
    try:
        logger.info(f"Inserting master data into the table :- {mastertableData}" )
        if mastertableData == "genre":
            for k,v in masterData.items():
                cursor.execute(genreTableInsertQuery,k,v)
        elif mastertableData == "stars":
            for k,v in masterData.items():
                cursor.execute(starsTableInsertQuery,k,v)
            conn.commit()
            logger.info(f"Inserting master data into the table :- {mastertableData} is complete" )
    except Exception as ex:
        logger.error(f"An error occurred while inserting data into the table - {mastertableData}")
        raise


def InsertTransactionData(mastertableName,data :pd.DataFrame):
    """
        purpose : This method is responsible for inserting data into the transaction table; movie, movie_genre &
                movie_star
        args : mastertableName -> string : table name
                data -> dataframe : transaction data for the table
        return : N/A
    """
    try:
        logger.info(f"Inserting Transaction data for the table movie_{mastertableName}")
        if(mastertableName == "genre"):
            for index, row in data.iterrows():
                cursor.execute(genreMovieRelTableINSERTQuery, (row["genre"]), str(row["ID"]))
        
        elif (mastertableName == "stars"):
            for index, row in data.iterrows():
                
                cursor.execute(starsMovieRelTableINSERTQuery, str(row["ID"]), row["stars"])
        else:
            for index, row in data.iterrows():
                print(type(row["director"]))
                print(row["director"])
                cursor.execute("""INSERT INTO IMDB.movie (movie_id,title,director,rating,certificate_rating, runtime, votes, description)
                            VALUES (?,?,?,?,?,?,?,?)""", 
                            row["ID"],
                            row["movie"],
                            row["director"] if isinstance(row["director"], str) else None,
                            row["rating"] if isinstance(row["rating"], str) else None,
                            row["certificate"] if isinstance(row["certificate"], str) else None,
                            row["runtime"] if isinstance(row["runtime"], str) else None,
                            row["votes"] if isinstance(row["votes"], str) else None,
                            row["description"] if isinstance(row["description"], str) else None,
                            )

        conn.commit()
        logger.info(f"Inserting Transaction data for the table movie_{mastertableName} is complete")
    except Exception as ex:
        logger.error(f"An error occured while inserting data into the table - movies_{mastertableName}")
        raise



def ConnectionManager(connType):
    """
        purpose : This method is responsible for managing the database connection
        args : connType -> string : type of connection
        return : N/A
    """
    if(connType == "OPEN"):
        OpenDatabaseConnection()
    elif (connType == "CLOSE"):
        CloseDatabaseConnection()





