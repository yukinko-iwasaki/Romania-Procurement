# Databricks notebook source
import requests
import pandas as pd

DEST_FOLDER = "/Volumes/prd_mega/sprocu92/vprocu92/Documents/raw/"
DEST_TABLE = 'prd_mega.sprocu92.raw_contracts'
LIST_TABLE = 'prd_mega.sprocu92.contract_list'
DEST_ALL_TABLE = 'prd_mega.sprocu92.contract_bronze'


# COMMAND ----------

# MAGIC %md
# MAGIC ## Get the list of relevant documents 
# MAGIC
# MAGIC a. Business Type: ALL
# MAGIC
# MAGIC b. Dates : 2022/01/01 - 2025/10/6
# MAGIC
# MAGIC c. Document types - Invitation to Tender/ Notice of Participation/Concession Notices

# COMMAND ----------

def get_payload(year):
    PAGE_SIZE = 1000

    payloads = {
        "sysNoticeTypeIds": [2, 7, 12], # 2 for Notice of Participation, 7 for concession notice, #12 for invitation to tender
        "sortProperties": [],
        "pageSize": PAGE_SIZE,
        "hasUnansweredQuestions": False,
        "startPublicationDate": f"{year}-01-01T20:02:15.771Z",
        "endPublicationDate": f"{year+1}-01-01T05:00:00.000Z",
        "sysProcedureStateId": None,
        "pageIndex": 0,
    }
    return payloads

# COMMAND ----------

import json, calendar, datetime, requests, pandas as pd

PAGE_SIZE = 2000
MIN_YEAR = 2022
MAX_YEAR = 2026
def get_payload(start_date, end_date, page_index):
    """Build payload for a given date window and page offset."""
    return {
        "sysNoticeTypeIds": [2, 7, 12],          # 2: Notice of Participation, 7: concession, 12: Invitation to Tender
        "pageSize": PAGE_SIZE,
        "hasUnansweredQuestions": False,
        "startPublicationDate": f"{start_date}T00:00:00.000Z",
        "endPublicationDate": f"{end_date}T23:59:59.999Z",
        "sysProcedureStateId": None,
        "pageIndex": page_index,                 # offset = page_index * pageSize
        "sortProperties":[{"sortProperty":"publicationDate","descending":True},{"sortProperty":"estimatedValue","descending":True}]}
url = "https://www.e-licitatie.ro/api-pub/NoticeCommon/GetCNoticeList/"
headers = {
    "Content-Type": "application/json",
    "Referer": "https://e-licitatie.ro/pub/notices/contract-notices/list/0/0",
}

all_results = []
# Extracting the list of notice metadata. Note that the API returns 3000 items per call (server limit), we need to extract per month to avoid reaching the limit.
for year in range(MIN_YEAR, MAX_YEAR + 1):
    for month in range(1, 13):
        # stop if we have passed the max year/month
        if year == MAX_YEAR and month > 12:
            break

        start_str = f"{year}-{month:02d}-01"
        last_day = calendar.monthrange(year, month)[1]
        end_str = f"{year}-{month:02d}-{last_day}"

        page_index = 0
        while True:
            payload = get_payload(start_str, end_str, page_index)
            resp = requests.post(url, json=payload, headers=headers)

            if resp.status_code != 200:
                print(f"Failed request for {year}-{month:02d} page {page_index}: {resp.status_code}")
                break

            data = json.loads(resp.content)
            total = data.get('total', 0)
            if total == 3000 and page_index == 0:
                print(f"Alert for {year}-{month:02d}: {total} items returned")

            items = data.get("items", [])
            if not items:
                # no more data for this month
                break

            # if fewer than PAGE_SIZE items returned
            all_results.extend(items)

            print(f"{year}-{month:02d} page {page_index} → {len(items)} new items (total {len(all_results)})")

            # if fewer than PAGE_SIZE items returned, this is the last page for the month
            if len(items) < PAGE_SIZE:
                break

            page_index += 1

# Convert to pandas DataFrame (or Spark DataFrame later)
df = pd.DataFrame(all_results)

# Some issue on the server side database. There were duplicate items even in one single API call(likely a data integrity issue rather than API pagination issue). Let's remove them. 
print(f"Total number of records: {len(df)}")
df.drop_duplicates(subset='cNoticeId', keep='first', inplace=True)
print(f"Total number of records after deduplication: {len(df)}")

spark_df = spark.createDataFrame(df)
spark_df.write.mode("overwrite").saveAsTable(LIST_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get the information for each of the contract
# MAGIC

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


contract_list = spark.read.table(LIST_TABLE).select(['sysNoticeTypeId', 'cNoticeId']).toPandas()
contract_list = contract_list[contract_list['sysNoticeTypeId'].isin([2,7])] # 12 invitation to tender has different API endpoints for extracting
contract_list.sort_values(by='cNoticeId', inplace=True)

# To handle the API rate limit, adjust start_index to restart the extraction execution. 
# List of document_ids to loop through
BATCH_DF_SIZE = 100  
start_index = 0 # Change as needed
results = []
for idx, row in enumerate(contract_list[start_index:].itertuples(), start=start_index):
    
    contract_id = row.cNoticeId
    row_data = [contract_id]
    document_type = row.sysNoticeTypeId

    
    # Get general info to extract dfNoticeId and save general data
    general_info_url = f'https://www.e-licitatie.ro/api-pub/comboPub/getNoticeGeneralInfo/?noticeId={contract_id}&sysNoticeTypeId={document_type}'
    try:

        general_response = http.get(
                    general_info_url,
                    headers={
                        "Referer": f"https://www.e-licitatie.ro/pub/notices/c-notice/v2/view/{contract_id}"
                    },
                )
        general_data = json.loads(general_response.content.decode("utf-8"))
        dfNoticeId = general_data.get('dfNoticeId', 111)  # Default to 111 if not found
        row_data.append(general_response.content.decode("utf-8"))
            
    except Exception as e:
        print(f"Failed to get general info for {contract_id}: {e}")
        dfNoticeId = 111  # Fallback to default
    
    urls = {
        "section1": f"https://www.e-licitatie.ro/api-pub/NoticeCommon/GetSection1View/?initNoticeId={contract_id}&sysNoticeTypeId={document_type}",
        "section2": f"https://www.e-licitatie.ro/api-pub/NoticeCommon/GetSection21View/?dfNoticeId={dfNoticeId}&initNoticeId={contract_id}&sysNoticeTypeId={document_type}",
        "section3": f"https://www.e-licitatie.ro/api-pub/NoticeCommon/GetSection3View/?initNoticeId={contract_id}&sysNoticeTypeId={document_type}",
        "section4": f"https://www.e-licitatie.ro/api-pub/NoticeCommon/GetSection4View/?initNoticeId={contract_id}&sysNoticeTypeId={document_type}",
        "section6": f"https://www.e-licitatie.ro/api-pub/NoticeCommon/GetSection6View/?initNoticeId={contract_id}&sysNoticeTypeId={document_type}",
    }
    for section, url in urls.items():
        response = http.get(
            url,
            headers={
                "Referer": f"https://www.e-licitatie.ro/pub/notices/c-notice/v2/view/{contract_id}"
            },
        )
        try:
            data = response.content.decode("utf-8")
        except Exception as e:
            print(f"Failed to parse JSON for {section} of {contract_id}: {e}")
        row_data.append(data)
    results.append(row_data)

    # --- 3. Batch Write to Delta Lake ---
    if (len(results) >= BATCH_DF_SIZE) or (idx + 1 == len(contract_list)):
        if results: # Check if there is data to write
            # Create the DataFrame using the explicit raw_data_schema
            schema = StructType([
                StructField("documentId", StringType(), True),
                StructField("generalInfo", StringType(), True),
                StructField("section1", StringType(), True),
                StructField("section2", StringType(), True),
                StructField("section3", StringType(), True),
                StructField("section4", StringType(), True),
                StructField("section6", StringType(), True),
            ])
            df_batch = spark.createDataFrame(results, schema=schema)

            # Write the DataFrame to the Delta Lake path
            df_batch.write \
                    .format("delta") \
                    .mode("append") \
                    .saveAsTable(DEST_ALL_TABLE)
            
            print(f"Saved batch {idx // BATCH_DF_SIZE + 1} to Delta Lake.")
            results = [] # Reset

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

df.shape

# COMMAND ----------

contract_list.shape

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

df = spark.read.table(DEST_ALL_TABLE)
contract_list = spark.read.table(LIST_TABLE).select(['sysNoticeTypeId', 'cNoticeId']).toPandas()
contract_list = contract_list[contract_list['sysNoticeTypeId'].isin([2,7])] # 12 invitation to tender has different API endpoints for extracting
contract_list.sort_values(by='cNoticeId', inplace=True)
df = df.toPandas()
df.documentId = df.documentId.astype(int)
df2 = df[df.documentId.isin(contract_list.cNoticeId)]
df_batch = spark.createDataFrame(df2)
df_batch.write.mode("overwrite").saveAsTable(DEST_ALL_TABLE)

# COMMAND ----------

from pyspark.sql.functions import col

# 1. Read the main table (stays distributed on executors)
df = spark.read.table('prd_mega.sprocu92.raw_contracts_all')

# 2. Read the list table (can be small, but keep it as Spark for now)
#    We only need 'cNoticeId' and filter on 'sysNoticeTypeId'.
contract_list = spark.read.table(LIST_TABLE).select('sysNoticeTypeId', 'cNoticeId')
contract_list = contract_list.filter(col('sysNoticeTypeId').isin([2, 7]))

# 3. Create the lookup list for the join (THIS IS THE CRITICAL CHANGE)
#    Since this list is small, we collect it to the driver and BROADCAST it.
#    If 'contract_list' is large, this step should be skipped for a standard join.
#    Assuming 'contract_list' is small enough to fit on a single machine.

# Get the list of IDs from the DataFrame (small operation on driver)
cNoticeId_list = [row['cNoticeId'] for row in contract_list.collect()] 

# 4. Filter the main DataFrame using the list (efficient filtering)
df_batch = df.filter(col('documentId').isin(cNoticeId_list))

# 5. Write the result (Now the task sent to executors is small and clean)
# df_batch.write.mode("overwrite").saveAsTable(DEST_ALL_TABLE)

# COMMAND ----------

df_batch.count()


# COMMAND ----------

DEST_ALL_TABLE

# COMMAND ----------

df_batch.write.mode("overwrite").saveAsTable(DEST_ALL_TABLE)

# COMMAND ----------

test = df_batch.toPandas()

# COMMAND ----------


