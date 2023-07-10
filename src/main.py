import pandas as pd
from fuzzywuzzy import fuzz
from collections import defaultdict

Source_path=r"C:\Users\Nivetha Vijayakumar\Downloads\source_1_2.csv"
Cleansed_path=r"C:\Users\Nivetha Vijayakumar\Downloads\Biodata2.csv"
Target_path=r"C:\Users\Nivetha Vijayakumar\Downloads\Biodata3.csv"


data = pd.read_csv(Source_path)

df = data.drop_duplicates(subset='iban',keep='first')
df['iban'] = df['iban'].replace('INVALID', pd.NA)

df.to_csv(Cleansed_path,index=False)


df1 = pd.read_csv(Cleansed_path)


# Function to find similar company names and assign group names
def assign_company_group(df1):
    company_groups = defaultdict(str)
    for index, row in df1.iterrows():
        name = row['name']
        for group, group_name in company_groups.items():
            if fuzz.ratio(name.lower(), group_name.lower()) > 80:
                df1.at[index, 'company_group'] = group
                break
        else:
            company_groups[name] = name
            df1.at[index, 'company_group'] = name

    return df1


# Apply the function to assign company groups
df1 = assign_company_group(df1)

df1.to_csv(Target_path,index=False)
# Print the updated DataFrame
print(df1)