# Databricks notebook source
import requests
import pandas as pd

DEST_FOLDER = "/Volumes/prd_mega/sprocu92/vprocu92/Documents/raw/"
DEST_TABLE = 'prd_mega.sprocu92.raw_contracts'
LIST_TABLE = 'prd_mega.sprocu92.contract_list'
FILE_LIST = 'prd_mega.sprocu92.FILE_LIST'


# COMMAND ----------

contract_list = spark.read.table(LIST_TABLE).select(['sysNoticeTypeId', 'cNoticeId']).toPandas()
contract_list = contract_list[contract_list['sysNoticeTypeId'].isin([2])] 
contract_list.sort_values(by='cNoticeId', inplace=True)

# COMMAND ----------

import json
import pandas as pd
import requests

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from pyspark.sql.types import StructType, StructField, StringType


retry_strategy = Retry(
    total=3,
    backoff_factor=10
)
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("https://", adapter)
http.mount("http://", adapter)




# To handle the API rate limit, adjust start_index to restart the extraction execution. 
# List of document_ids to loop through
BATCH_DF_SIZE = 10  
start_index = 0 # Change as needed
results = []
FILES_LIST_URL = 'https://www.e-licitatie.ro/api-pub/NoticeCommon/GetDfNoticeSectionFiles'
for idx, row in enumerate(contract_list[start_index:].itertuples(), start=start_index):
    
    contract_id = row.cNoticeId
    row_data = [contract_id]
    document_type = row.sysNoticeTypeId
    try:
        files_response = http.get(
                    f'{FILES_LIST_URL}/?initNoticeId={contract_id}&sysNoticeTypeId={document_type}',
                    headers={
                        "Referer": f"https://www.e-licitatie.ro/pub/notices/c-notice/v2/view/{contract_id}"
                    },
                )
        files_list_data = json.loads(files_response.content.decode("utf-8"))
        row_data.append(files_list_data)
        results.append(row_data)
    except Exception as e:
        print(f"Failed to get general info for {contract_id}: {e}")

    # --- 3. Batch Write to Delta Lake ---
    if (len(results) >= BATCH_DF_SIZE) or (idx + 1 == len(contract_list)):
        break
        if results: # Check if there is data to write
            # Create the DataFrame using the explicit raw_data_schema
            schema = StructType([
                StructField("documentId", StringType(), True),
                StructField("filelist", StringType(), True)
            ])
            df_batch = spark.createDataFrame(results, schema=schema)
        
            # Write the DataFrame to the Delta Lake path
            df_batch.write \
                    .format("delta") \
                    .mode("append") \
                    .saveAsTable(FILE_LIST)
            
            print(f"Saved batch {idx // BATCH_DF_SIZE + 1} to Delta Lake.")
            results = [] # Reset

# COMMAND ----------

import json
import os
BASE_PATH = '/Volumes/prd_mega/sprocu92/vprocu92/Workspace/contract_files'

# COMMAND ----------

BASE_URL = 'https://www.e-licitatie.ro'
for contract_id, files_list  in results:
    for doc in files_list['dfNoticeDocs']:
        try:
            files_response = http.get(
                        f'{BASE_URL}/{doc["noticeDocumentUrl"]}',
                        headers={
                            "Referer": f"https://www.e-licitatie.ro/pub/notices/c-notice/v2/view/{contract_id}"
                        },)
            write_folder_name = f'{BASE_PATH}/{contract_id}'
            if not os.path.exists(write_folder_name):
                os.makedirs(write_folder_name)
            with open(f'{write_folder_name}/{doc["noticeDocumentName"]}', 'wb') as f:
                f.write(files_response.content)
        except Exception as e:
            print(f"Failed to get general info for {contract_id}: {e}")

# COMMAND ----------

pip install asn1crypto


# COMMAND ----------

WRITE_BASE_PATH = '/Volumes/prd_mega/sprocu92/vprocu92/Workspace/contract_files_unpacked'

# COMMAND ----------

from asn1crypto import cms
# unpacking the signature files
for contract in os.listdir(BASE_PATH):
    for doc in os.listdir(f'{BASE_PATH}/{contract}'):
        read_file = f'{BASE_PATH}/{contract}/{doc}'
        write_folder_name = f'{WRITE_BASE_PATH}/{contract}'

        if not os.path.exists(write_folder_name):
            os.makedirs(write_folder_name)
        if read_file.endswith('.p7m') or read_file.endswith('.p7s'):
            unpacked_doc = os.path.splitext(doc)[0]
            write_file = f'{write_folder_name}/{unpacked_doc}'
            # 1. Read the binary data
            with open(read_file, "rb") as f:
                file_data = f.read()

            # 2. Parse the CMS (Content Info) structure
            content_info = cms.ContentInfo.load(file_data)

            # 3. Access the 'content' field. 
            # Most p7m files are 'signed_data'
            compressed_data = content_info['content']

            # 4. Extract the actual payload (Encapsulated Content)
            # This is usually where the XML, PDF, or text lives
            payload = compressed_data['encap_content_info']['content'].native
        else:
            write_file = f'{write_folder_name}/{doc}'
            with open(read_file, "rb") as src_file:
                payload = src_file.read()
        with open(write_file, "wb") as f:
            f.write(payload)

# COMMAND ----------


