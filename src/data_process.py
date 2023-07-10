import sys
import pandas as pd
from fuzzywuzzy import fuzz
from collections import defaultdict
from src.logger import logger


Source_path=r"C:\Users\Nivetha Vijayakumar\Downloads\source_1_2.csv"
Cleansed_path=r"C:\Users\Nivetha Vijayakumar\Downloads\Cleansed.csv"
Target_path=r"C:\Users\Nivetha Vijayakumar\Downloads\Processed.csv"


def Import_Data(file_type, Source_path):
    try:
        logger.info(f"Reading Input {file_type} File")
        Input_Data = pd.read_csv(Source_path)
        logger.info("Input File Read Successfully")

        return Input_Data

    except Exception as e:
        logger.exception(f"Error in reading data {str(e)}")
        sys.exit(400)

def Data_Process(file_type, Source_path):
    try:
        logger.info(f"Started data cleansing")
        Input_Data = Import_Data(file_type, Source_path)
        # Cleaning the input data to remove duplicates
        Processed_DF = Input_Data.drop_duplicates(subset='iban',keep='first')
        # Cleaning the input data to replace invalid columns with na
        Processed_DF['iban'] = Processed_DF['iban'].replace('INVALID', pd.NA)
        Processed_DF.to_csv(Cleansed_path,index=False,na_rep='NaN')
        logger.info("Data Cleansing Successfully")

        return Processed_DF

    except Exception as e:
        logger.exception(f"Error during cleansing the data {str(e)}")
        sys.exit(400)


def assign_company_group(Processed_DF):
    try:
        # Applying entity resolution fun on the cleansed data
        logger.info(f"Initiated Entity Resolution task")
        Processed_DF = pd.read_csv(Cleansed_path)
        company_groups = defaultdict(str)
        for index, row in Processed_DF.iterrows():
            name = row['name']
            for group, group_name in company_groups.items():
                if fuzz.ratio(name.lower(), group_name.lower()) > 80:
                    Processed_DF.at[index, 'company_group'] = group
                    break
            else:
                company_groups[name] = name
                Processed_DF.at[index, 'company_group'] = name
        logger.info("Entity Resolution task Successfully")

        return Processed_DF

    except Exception as e:
        logger.exception(f"Error in reading data {str(e)}")
        sys.exit(400)



def Data_Load(file_type, Target_path):
    try:
        # Load processed data and write to target path
        logger.info(f"Writing Data to {Target_path}")
        Processed_DF = pd.read_csv(Cleansed_path)
        Processed_DF = assign_company_group(Processed_DF)
        Processed_DF.to_csv(Target_path, index=False,na_rep='NaN')
        logger.info("Saved File Successfully")

    except Exception as e:
        logger.exception(f"Error in uploading data {str(e)}")
        sys.exit(400)