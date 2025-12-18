# Databricks notebook source
import requests
import pandas as pd
from pyspark.sql.functions import col
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from pyspark.sql.types import StructType, StructField, StringType
DEST_FOLDER = "/Volumes/prd_mega/sprocu92/vprocu92/Documents/raw/"
DEST_TABLE = 'prd_mega.sprocu92.raw_contracts'
LIST_TABLE = 'prd_mega.sprocu92.contract_list'
DEST_ALL_TABLE = 'prd_mega.sprocu92.contract_bronze'
PARTICIPANTS_BRONZE = 'prd_mega.sprocu92.participants_list_bronze'
AWARD_BRONZE = 'prd_mega.sprocu92.award_notice_bronze'
AWARDED = 'Atribuita'

# COMMAND ----------

contract_list = spark.read.table(LIST_TABLE)
contract_list = contract_list.select(
    col('cNoticeId').cast('string').alias('cNoticeId'),
    col('noticeId').cast('string').alias('noticeId'),
    col('procedureId').cast('string').alias('procedureId'),
    col('noticeNo').cast('string').alias('noticeNo'),
    col('sysNoticeTypeId').cast('string').alias('sysNoticeTypeId'),
    col('sysNoticeState').cast('string').alias('sysNoticeState'),
    col('contractingAuthorityNameAndFN').cast('string').alias('contractingAuthorityNameAndFN'),
    col('contractTitle').cast('string').alias('contractTitle'),
    col('sysAcquisitionContractType').cast('string').alias('sysAcquisitionContractType'),
    col('sysProcedureType').cast('string').alias('sysProcedureType'),
    col('sysContractAssigmentType').cast('string').alias('sysContractAssigmentType'),
    col('cpvCodeAndName').cast('string').alias('cpvCodeAndName'),
    col('estimatedValueRon').cast('string').alias('estimatedValueRon'),
    col('isOnline').cast('string').alias('isOnline'),
    col('hasLots').cast('string').alias('hasLots'),
    col('noticeStateDate').cast('string').alias('noticeStateDate'),
    col('minTenderReceiptDeadline').cast('string').alias('minTenderReceiptDeadline'),
    col('maxTenderReceiptDeadline').cast('string').alias('maxTenderReceiptDeadline'),
    col('errataNo').cast('string').alias('errataNo'),
    col('sysNoticeVersionId').cast('string').alias('sysNoticeVersionId'),
    col('tenderReceiptDeadlineExport').cast('string').alias('tenderReceiptDeadlineExport'),
    col('estimatedValueExport').cast('string').alias('estimatedValueExport'),
    col('sadId').cast('string').alias('sadId'),
    col('hasAppeal').cast('string').alias('hasAppeal'),
    col('sysProcedureState.text').alias('sysProcedureStateText'),
).toPandas()

contract_list = contract_list[(contract_list['sysProcedureStateText'] == AWARDED) & (contract_list['sysNoticeTypeId'] == '2')]
contract_list.sort_values(by='cNoticeId', inplace=True)


# COMMAND ----------

import json
import pandas as pd
import requests

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
BATCH_DF_SIZE = 1000
start_index = 0 # Change as needed
results = []
for idx, row in enumerate(contract_list[start_index:].itertuples(), start=start_index):
    
    contract_id = row.cNoticeId
    row_data = [contract_id]

    
    # Get general info to extract dfNoticeId and save general data
    central_info = f'https://www.e-licitatie.ro/api-pub/PUBLICCNotice/getPubCNoticeView/?cNoticeId={contract_id}'
    try:

        central_response = http.get(
                    central_info,
                    headers={
                        "Referer": central_info
                    },
            )
        central_data = json.loads(central_response.content.decode("utf-8"))
        procedureId = central_data.get('procedureId', 111)  # Default to 111 if not found
        row_data.append(central_data)
            
    except Exception as e:
        print(f"Error for contract_id {contract_id}: {e}")
        continue
    
    procurement_url = f"https://www.e-licitatie.ro/api-pub/PUBLICProcedure/GetProcedureReports/?procedureId={procedureId}"

    try:
        procurement_response = http.get(
                    procurement_url,
                    headers={
                        "Referer": procurement_url
                    },
                )
        procurement_info = json.loads(procurement_response.content.decode("utf-8"))
        statements = procurement_info.get('items', [])
        participation_list = []
        statement_list =[]
        for statement in statements:            
            statement_list.append(statement)
            procedureStatementId = statement.get('procedureStatementId')
            participation_url = f'https://www.e-licitatie.ro/api-pub/PUBLICProcedure/GetProcedureStatementView/?procedureStatementId={procedureStatementId}'
            participation_response = http.get(
                participation_url,
                headers={
                    "Referer": participation_url
                },
            )
            participation_info = participation_response.content.decode("utf-8")
            participation_list.append(participation_info)
        
        row_data.append(participation_list)
        row_data.append(participation_list)
    except Exception as e:
        print(f"Error for contract_id {contract_id}: {e}")
        continue

    results.append(row_data)

    # --- 3. Batch Write to Delta Lake ---
    if (len(results) >= BATCH_DF_SIZE) or (idx + 1 == len(contract_list)):
        if results: # Check if there is data to write
            # Create the DataFrame using the explicit raw_data_schema
            schema = StructType([
                StructField("documentId", StringType(), True),
                StructField("central_data", StringType(), True),
                StructField("statements", StringType(), True),
                StructField("participation", StringType(), True),
            ])
            df_batch = spark.createDataFrame(results, schema=schema)

            # Write the DataFrame to the Delta Lake path
            df_batch.write \
                    .format("delta") \
                    .mode("append") \
                    .saveAsTable(PARTICIPANTS_BRONZE)
            
            print(f"Saved batch {idx // BATCH_DF_SIZE + 1} to Delta Lake.")
            results = [] # Reset

# COMMAND ----------

import json
awarded_list = contract_list[contract_list['sysProcedureStateText'] == AWARDED]
# Get awards 
raw_contracts = spark.table(DEST_ALL_TABLE).select(['documentId', 'generalInfo']).toPandas()
raw_contracts = raw_contracts[raw_contracts['documentId'].isin(awarded_list.cNoticeId.values)]
raw_contracts.sort_values(by='documentId', inplace=True)

# COMMAND ----------

import requests
AWARD_URL = f"https://www.e-licitatie.ro/api-pub/C_PUBLIC_CANotice/GetCANoticeContracts"
results = []
http = requests.Session()
BATCH_DF_SIZE = 1000
for idx, row in enumerate(raw_contracts.itertuples(), start=0):
    contract_id = row.documentId
    caNoticeId = json.loads(row.generalInfo).get("caNoticeId")
    row = [contract_id, caNoticeId]

    payloads = {
        "caNoticeId": caNoticeId,
        "contractNo": None,
        "winnerTitle": None,
        "winnerFiscalNumber": None,
        "contractDate": {"from": None, "to": None},
        "contractValue": {"from": None, "to": None},
        "contractMinOffer": {"from": None, "to": None},
        "contractMaxOffer": {"from": None, "to": None},
        "contractTitle": None,
        "lots": None,
        "sortOrder": [],
        "sysContractFrameworkType": {},
        "skip": "0",
        "take": "5",
    }

    res = http.post(
        AWARD_URL,
        json=payloads,
        headers={
            "Referer": "https://www.e-licitatie.ro/pub/notices/ca-notices/view-c/100593302"
        },
    )
    award_info = res.content.decode("utf-8")
    row.append(award_info)
    results.append(row)
      # --- 3. Batch Write to Delta Lake ---
    if (len(results) >= BATCH_DF_SIZE) or (idx + 1 == len(contract_list)):
        if results: # Check if there is data to write
            # Create the DataFrame using the explicit raw_data_schema
            schema = StructType([
                StructField("documentId", StringType(), True),
                StructField("caNoticeId", StringType(), True),
                StructField("award_info", StringType(), True),
            ])
            df_batch = spark.createDataFrame(results, schema=schema)

            # Write the DataFrame to the Delta Lake path
            df_batch.write \
                    .format("delta") \
                    .mode("append") \
                    .saveAsTable(AWARD_BRONZE)
            
            print(f"Saved batch {idx // BATCH_DF_SIZE + 1} to Delta Lake.")
            results = [] # Reset


# COMMAND ----------


