# Entity_Resolution_Python
Data Pipeline for Duplicate Removal and Entity Resolution

Objective: The Purpose of this code is to create a ETL pipeline which removes duplicate and perform entity resolution for the given dataset.

Source:
An csv file is given to retrieve data which act as source. Our ETL pipeline should read the csv data and do the data cleaning activity and store it in processed space.

Methodology: 
To achieve all this I have used python pandas module to read the file and convert it to dataframes. 
For dropping the duplicates I have used drop_duplicates function in pandas which is drop the later duplicates records by holding the first.
For handling the invalid columns I have replaced the column values with Nan for effictively handling it in future.

For entity resolution task we have used fuzzywuzzy library in python. What it does is 
It will compares the current company name with the existing company names using fuzzy string matching. The fuzz.ratio() function from the fuzzywuzzy library is used to calculate the similarity ratio between two strings. If the similarity ratio is above 80 (indicating a relatively high similarity), the current record is considered to belong to the corresponding group.
If a matching group is found, the 'company_group' column of the current row in the Processed file is updated with the group name.

Execution Steps: 
To run the code:Update the Source_path to your source data. From the terminal navigate to the requirements.txt. Run the following command to install all the necessary pip packages to run the program: sudo pip install -r requirements.txt 
To run the Pipeline, run the following command: python app.py To view the results of the ETL transformation, find the newly created file processed data in the Target_path.
