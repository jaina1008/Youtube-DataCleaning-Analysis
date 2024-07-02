import awswrangler as wr
import pandas as pd
import urllib.parse
import os

os_input_s3_cleaned_layer = os.environ['s3_cleaned_layer']
os_input_glue_catalog_db_name = os.environ['glue_catalog_db_name']
os_input_glue_catalog_table_name = os.environ['glue_catalog_table_name']
os_input_write_data_operation = os.environ['write_data_operation']

def lambda_handler(event, context):
    # get the object from event and show it's content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        
        # Creating DF from context
        df_raw = wr.s3.read_json('s3://{}/{}'.format(bucket, key))
        
        # Extract required columns: