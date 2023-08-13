import pandas as pd
import ast
from loggingHelper import logger


def DropColumnFromDataFrame(column,data : pd.DataFrame):
    """
        purpose : This method is responsible for droping the columns - genre, stars from the untransformed data
        args : data -> dataframe : contains the untransformed data from the csv file
                column -> string : contains name of the column to be dropped
        return : data -> dataframe : contains the data after the columns have been dropped
    """
    try:
        data = data.drop(columns=[column])
        return data
    except Exception as ex:
        logger.error(f"An error occured while executing DropColumnFromDataFrame() for column name - {column}")
        raise 


def map_values_to_ids(values, data : dict ):
    """
        purpose : This method is responsible for mapping the transaction data to their id from the master table
        args : data -> dict : contains data from the master table
                values -> series : contains row of data that is to be mapped to the respective value from the master table
        return : series containing the tranformed value
    """
    try:
        return ','.join(str(data[v.strip()]) for v in values.split(','))
    except Exception as ex:
        logger.error(f"An error occurred while executing map_vales_to_ids() for the series of value :- {values}")
        raise


def convert_to_list(val):

    """
        purpose : This method is responsible for checking if the passed value is a series or not
        args : val -> series : contains row of data that is to be checked
        return : the evaluated value or null if their is any issue
    """
    try:
        return ast.literal_eval(val)
    except (SyntaxError, ValueError):
        return []
    
def clean_and_join(lst):
    """
        purpose : This method is responsible for removing all the empty values
        args : lst -> list : contains the list from the data to be transformed
        return : the filtered list containing the non-null values
    """
    try:
        return ''.join(filter(None, (item.strip() for item in lst if item.strip())))
    except Exception as ex:
        logger.error(f"An error occured while executing the function clean_and_join() for the list - {lst}")
        raise