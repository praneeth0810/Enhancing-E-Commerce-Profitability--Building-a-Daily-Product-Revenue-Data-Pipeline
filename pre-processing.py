'''
Import all the libraries required for processing the data
pandas for accessing the data files
os to validate if it is a file type
glob to get the list of file names
json to 

perform pip install pandas,gcsfs,pyarrow, glob2

'''
import pandas as pd
import os
import glob
import json

'''

This function retreives a list of file names by taking the path of the files as input
glob function is used to get the list of files in that directory
we use a lambda function to filter out the files which are of type 'file' and ends with 'part-0000'

'''

def get_file_names(src_base_dir):
    items = glob.glob(f'{src_base_dir}/**', recursive=True)
    return list(filter(lambda item: os.path.isfile(item) and item.endswith('part-00000'), items))

'''
This function retreives the column name using the schema.json file path and ds names
First, it loads the schemas from the json file, and we will sort the columns according to their positions
Then we store each col name in a list and return it 
'''

def get_column_names(schemas_file, ds_name):
    schemas = json.load(open(schemas_file))
    ds_schema = sorted(schemas[ds_name], key=lambda col: col['column_position'])
    columns = [col['column_name'] for col in ds_schema]
    return columns

'''
Define the source, target, schema and bucket paths
'''
    
src_base_dir = '/home/managedata62/data/retail_db'
tgt_base_dir = 'retail_db_parquet'
schemas_file = '/home/managedata62/data/retail_db/schemas.json'
bucket = 'retailprodrev'

'''
Processing of data files
we first get the files names using defined function as above
we iterate each file
- we first get the suffix for each file by fetching last two names of the file path eg : customers/part-00000 '
- we get the data file name of the by fetching the last second name of the file
- we create a blob object name
- we will fetch the column names using the defined function
- convert the files to a dataframe and then write it into parquet format
- we use pandas here to add the column headers
'''

files = get_file_names(src_base_dir)
for file in files:
    print(f'Uploading file {file}')
    blob_suffix = '/'.join(file.split('/')[-2:])
    ds_name = file.split('/')[-2]
    blob_name = f'gs://{bucket}/{tgt_base_dir}/{blob_suffix}.snappy.parquet'
    columns = get_column_names(schemas_file, ds_name)
    df = pd.read_csv(file, names=columns)
    df.to_csv(blob_name, index=False)

print("Data shape of original files")

for ds in [
    'departments', 'categories', 'products',
    'customers', 'orders', 'order_items'
]:
    df = pd.read_csv(f'/home/managedata62/data/retail_db{ds}/part-00000', header=None)
    print(f'''Shape of {ds} in local files system is {df.shape}''')

print("Data shape of parquet files")

for ds in [
    'departments', 'categories', 'products',
    'customers', 'orders', 'order_items'
]:
    df = pd.read_parquet(f'gs://{bucket}/{tgt_base_dir}/{ds}/part-00000.snappy.parquet')
    print(f'''Shape of {ds} in gcs is {df.shape}''')
