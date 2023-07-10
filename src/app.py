import sys
from src.logger import logger
from src.data_process import Import_Data, Data_Process,assign_company_group,Data_Load

#Dirctory used for source, cleansed, target
Source_path=r"C:\Users\Nivetha Vijayakumar\Downloads\source_1_2.csv"
Cleansed_path=r"C:\Users\Nivetha Vijayakumar\Downloads\Cleansed.csv"
Target_path=r"C:\Users\Nivetha Vijayakumar\Downloads\Processed.csv"

# entry point
if __name__ == "__main__":
    try:
        logger.info("Initiated ETL Task")
        # Read data
        load_df = Import_Data("csv", Source_path)
        # Cleanse data
        process_df = Data_Process("csv",Source_path)
        # entity resolution method
        entity_resolution=assign_company_group(process_df)
        # Load processed data
        pre_processed_df = Data_Load("csv", Target_path)

    except Exception as e:
        logger.exception(f"An error occurred while executing main function {str(e)}")
        sys.exit(400)
