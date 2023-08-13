import pandas as pd
from loggingHelper import logger


def ExtractDataProcess():
    """
        purpose : This method is responsible for extracting the data from the data source
        args : no args
        return : A Dataframe containing the data read from the data source
    """
    logger.info("Getting data from source...")
    path=r"C:\Users\rorao\Downloads\archive\IMBD - Copy.csv"
    try:
        csv_Data = pd.read_csv(path)
        return csv_Data
    except FileNotFoundError as ex:
        logger.error("Error has occured. Please check the below stack trace!")
        raise FileNotFoundError
    
    
    