
from ETL.etlManager import ETLManager
from loggingHelper import logger


def main():
    """
        Purpose : This method starts the ETL process
        args :no args
        return : N/A
    """
    ETLManager()

if __name__ == "__main__":

    """This is the entry point to the ETL pipeline
    """
    logger.info("Pipepline Execution Begins!")
    main()
    logger.info("Pipepline Execution Ended!")
    