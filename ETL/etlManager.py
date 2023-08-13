from ETL.extractProcess import ExtractDataProcess
from ETL.transform import TransformDataProcess
from ETL.load import ConnectionManager, DeleteData
from loggingHelper import logger


def ETLManager():
    """purpose : This method orchestrates the entire etl process.
        args : no args
        return : N/A
    """
    try:
        ConnectionManager("OPEN")
        DeleteData()
        TransformDataProcess(ExtractDataProcess())
        ConnectionManager("CLOSED")
    except Exception as e:
        logger.exception(e)
