# Databricks notebook source
import requests
import pandas as pd

DEST_FOLDER = "data/"
LIST_FILE = DEST_FOLDER + "contract_list.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get the list of relevant documents 
# MAGIC
# MAGIC a. Business Type: Construction
# MAGIC
# MAGIC b. Dates : 2022/01/01 - 2025/10/6
# MAGIC
# MAGIC c. Document types - Invitation to Tender/ Notice of Participation

# COMMAND ----------

# Send a POST request to a sample API endpoint and save the response as CSV

import json


url = "https://www.e-licitatie.ro/api-pub/NoticeCommon/GetCNoticeList/"
headers = {
    "Content-Type": "application/json",
    "Referer": "https://e-licitatie.ro/pub/notices/contract-notices/list/0/0",
}
PAGE_SIZE = 1200
payloads = {
    "sysNoticeTypeIds": [2, 12], # 2 for Notice of Participation, 12 for Invitation to Tender
    "sortProperties": [],
    "pageSize": PAGE_SIZE,
    "hasUnansweredQuestions": False,
    "startPublicationDate": "2022-01-01T20:02:15.771Z",
    "startTenderReceiptDeadline": "2022-01-01T20:02:15.771Z",
    "sysProcedureStateId": 2,
    "pageIndex": 0,
    "cPVCategoryId": 2 # 2 for Construction
}


response = requests.post(url, json=payloads, headers=headers)

df = pd.DataFrame(json.loads(response.content)['items'])
df.to_csv(LIST_FILE, index=False)
print(f"Response saved as {LIST_FILE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get the information for each of the contract
# MAGIC

# COMMAND ----------

import json
import pandas as pd
import requests
contract_list = pd.read_csv(LIST_FILE)
contract_list.sort_values(by='cNoticeId', inplace=True)
# List of document_ids to loop through
results = []
batch_size = 100  # Change as needed
start_batch = 0
start_index = batch_size * start_batch  # For batch 3, this is 200
for idx, row in enumerate(contract_list[start_index:].itertuples(), start=start_index):
    contract_id = row.cNoticeId
    document_type = row.sysNoticeTypeId

    row_data = {"document_id": contract_id}
    dfNoticeId = 111  # Adjust if needed for URL2
    urls = {
        "section1": f"https://www.e-licitatie.ro/api-pub/NoticeCommon/GetSection1View/?initNoticeId={contract_id}&sysNoticeTypeId={document_type}",
        "section2": f"https://www.e-licitatie.ro/api-pub/NoticeCommon/GetSection21View/?dfNoticeId={dfNoticeId}&initNoticeId={contract_id}&sysNoticeTypeId={document_type}",
        "section3": f"https://www.e-licitatie.ro/api-pub/NoticeCommon/GetSection3View/?initNoticeId={contract_id}&sysNoticeTypeId={document_type}",
        "section4": f"https://www.e-licitatie.ro/api-pub/NoticeCommon/GetSection4View/?initNoticeId={contract_id}&sysNoticeTypeId={document_type}",
        "section6": f"https://www.e-licitatie.ro/api-pub/NoticeCommon/GetSection6View/?initNoticeId={contract_id}&sysNoticeTypeId={document_type}",
    }
    for section, url in urls.items():
        response = requests.get(
            url,
            headers={
                "Referer": f"https://www.e-licitatie.ro/pub/notices/c-notice/v2/view/{contract_id}"
            },
        )
        try:
            data = json.loads(response.content.decode("utf-8"))
            # Flatten each key with section identifier
            for k, v in data.items():
                row_data[f"{section}_{k}"] = v
        except Exception as e:
            print(f"Failed to parse JSON for {section} of {contract_id}: {e}")
    results.append(row_data)

    # Save every batch_size rows to a distinct file
    if (idx + 1) % batch_size == 0 or (idx + 1) == len(contract_list):
        batch_df = pd.DataFrame(results)
        batch_file = f"{DEST_FOLDER}consolidated_contracts_batch_{idx // batch_size + 1}.csv"
        batch_df.to_csv(batch_file, index=False)
        print(f"Saved batch to {batch_file}")
        results = []  # Reset for next batch
