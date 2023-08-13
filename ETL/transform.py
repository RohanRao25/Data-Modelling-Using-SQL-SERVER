import pandas as pd
from helpers import map_values_to_ids, DropColumnFromDataFrame, clean_and_join, convert_to_list
from ETL.load import InsertMasterTableData, InsertTransactionData
from loggingHelper import logger


def CreateTablesRelData(DataList,relationData,columnName):
    """
        purpose : This method is responsible for creating the transaction data for the tables ; movie_genre , movie_Star
        args : DataList -> dictionary : contains the master data for the respective table
                relationData -> dataframe : contains the uncleaned transaction for the respective table
                columnName -> string : Contains the table/ column name
        return : N/A
    """
    print('Creating Relation Data for :'+ columnName)
    logger.info(f"Creating Relation Data for the table - {columnName}")
    reversedRelationList = {v:k for k,v in DataList.items()}
    try:
        for index, row in relationData.iterrows():
            relationData.at[index, columnName] = map_values_to_ids(row[columnName],reversedRelationList)
        
        #Can do the above code in the below way
        #relationData["genre"] = relationData["genre"].apply(lambda x: map_values_to_ids(x, reversedRelationList))

        
        if(columnName == "stars"):

            UpdatedRelationData = relationData.assign(stars=relationData[columnName].str.split(',')).explode(columnName)
        elif(columnName == "genre"):
            UpdatedRelationData = relationData.assign(genre=relationData[columnName].str.split(',')).explode(columnName)
        
        print(UpdatedRelationData)
        #InsertMasterData(columnName,UpdatedRelationData)
        InsertTransactionData(columnName, UpdatedRelationData)
        
        print('Relation Data process complete!')
        logger.info(f" Relation Data for the table - {columnName} has been created")
    except Exception as ex:
        logger.error(f"An error occured while creating Relation data for {columnName}. Please refer the logs for more details")
        raise


def CreateMasterData(columnName, csv_Data):
    """
        purpose : This method is responsible for creating master data for the table; stars, genre
        args : columnName -> string : contains the table/column name
                csv_Data -> dataframe : contains the untransformed data from the CSV file
        return : N/A
    """
    DataList = {}
    relationData = pd.DataFrame()
    relationData = csv_Data[["ID",columnName]].copy()
    try:
        logger.info(f"Creating Master Data for the table - {columnName}")
        DropColumnFromDataFrame(columnName,csv_Data)
        

        if(columnName == "stars"):
            relationData[columnName] = relationData[columnName].apply(convert_to_list)
            relationData[columnName] = relationData[columnName].apply(clean_and_join)
        
        i=1
        for element in relationData.loc[:,columnName].str.split(',').explode():
            
            if(isinstance(element, (int, float))):
                pass
            elif(element.strip() not in DataList.values()):
            
                DataList.update({i:element.strip()})
                i +=1
        print(DataList)
        InsertMasterTableData(columnName,DataList)
        CreateTablesRelData(DataList,relationData,columnName)
        logger.info(" Master Data for the table - {columnName} has been created")
    except Exception as ex:
        logger.error(f"An error occured in the proecess of creating master data for {columnName}. PLease refer to the logs for more details.")
        raise



def addPrimaryID(csv_Data):
    """
        purpose : This method is responsible for adding a primary ID to the untransformed data read from the CSV file
        args : csv_Data -> dataframe : contains the untransformed data from the csv file
        return : N/A
    """
    csv_Data.insert(0, 'ID', range(1, len(csv_Data) + 1))




def TransformDataProcess(csv_Data):
    """
        purpose : This method is responsible for transforming the extracted data
        args : csv_Data -> dataframe : contains the untransformed data from the csv file
        return : N/A
    """
    try:
        if("ID" not in csv_Data):
            addPrimaryID(csv_Data)
            print(csv_Data.columns)
            InsertTransactionData("", csv_Data)
        for column in ["genre","stars"]:
            CreateMasterData(column,csv_Data)
    except Exception as ex:
        raise ex

    