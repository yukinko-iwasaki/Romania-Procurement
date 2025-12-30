# Databricks notebook source
import pandas as pd
import json
import os

# COMMAND ----------

LIST_TABLE = 'prd_mega.sprocu92.contract_list'
DEST_ALL_TABLE = 'prd_mega.sprocu92.raw_contracts_all'

from pyspark.sql.functions import col
def create_contract_list(mapping_file, table_name):
    # 1. Open the file for reading ('r')
    with open(mapping_file, 'r', encoding='utf-8') as f:
        # 2. Use json.load() (singular) to parse the file content directly
        cvp_mapping = json.load(f)

    print("JSON file loaded successfully!")
    # print(data) # Uncomment this to see the loaded data


    contract_list = spark.read.table(LIST_TABLE)
    contract_list = contract_list.select(
    col("cNoticeId"),
    col("contractTitle"),
    col('cpvCodeAndName'),
    col("sysProcedureState.id").alias("sysProcedureState_id"),
    col("estimatedValueRon").alias("estimatedValueRon").cast('double')).toPandas()

    results = []
    for idx, row in enumerate(contract_list.itertuples()):
        cpvCodeAndName = row.cpvCodeAndName
        contract_id = row.cNoticeId
        contractTitle = row.contractTitle
        status = row.sysProcedureState_id
        value_estimate = row.estimatedValueRon
        for category, cvps in cvp_mapping.items():
            for cvp in cvps:
                if cpvCodeAndName.startswith(cvp):
                    results.append((contract_id, contractTitle, cpvCodeAndName, category, status, value_estimate))

    filtered_list = pd.DataFrame(results, columns=['cNoticeId', 'contractTitle', 'cpvCodeAndName', 'category', 'status', 'value_estimate'])
    spark_df = spark.createDataFrame(filtered_list)
    spark_df.write.mode("overwrite").saveAsTable(table_name)
    print(f"Table {table_name} created successfully!")


def create_contract_criteria_table(list_table_name, table_name):
    contract_list = spark.read.table(list_table_name)
    raw_contracts = spark.read.table(DEST_ALL_TABLE)
    filtered_contracts = raw_contracts.join(contract_list, on=raw_contracts.documentId == contract_list.cNoticeId, how='inner').toPandas()

    results = []
    criteria_keys = ['noticeID', 'lotID', 'noticeAwardCriteriaID', 'directProportional', 'noticeAwardCriteriaName', 'noticeAwardCriteriaDescription', 'algorithmDescription', 'noticeAwardCriteriaOrder', 'noticeAwardCriteriaWeight', 'isElectronicCriteria', 'isFrameworkAgreementCriteria']
    for index, row in filtered_contracts.iterrows():
        documrent_id = row['documentId']
        criterias = json.loads(row['section2'])['section22']
        if not criterias:
            continue
        short_description = criterias['shortDescription']

        for criteria in criterias['noticeAwardCriteriaList']:
            criteria_item = [criteria[key] for key in criteria_keys]
            results.append((documrent_id, short_description, *criteria_item ))
    criterias = pd.DataFrame(results, columns=['documentId', 'short_description', *criteria_keys])

    spark_df = spark.createDataFrame(criterias)
    spark_df.write.mode("overwrite").saveAsTable(table_name)
    print(f"Table {table_name} created successfully!")


# COMMAND ----------

# MAGIC %md
# MAGIC Create Criteria Table for Romania Project (Social Protection Focus)

# COMMAND ----------

LIST_TABLE_SOCIAL = 'prd_mega.sprocu92.social_contract_list'
SOCIAL_CONTRACT_CRITERIA = 'prd_mega.sprocu92.social_contract_criteria'

file_name = os.path.join("..",'auxiliary', 'social_cpv_mapping.json')
# Create a filtered list of the contracts which match the predefined list of CPVs 
create_contract_list(file_name, LIST_TABLE_SOCIAL)

#Parse the criteria for the filtered list of criterias for Romania Social Protection Project
create_contract_criteria_table(LIST_TABLE_SOCIAL, SOCIAL_CONTRACT_CRITERIA)

# COMMAND ----------

# MAGIC %md
# MAGIC Create Criteria Table for Romania Project (Social Enterprise Focus)

# COMMAND ----------

# MAGIC %ls

# COMMAND ----------

LIST_TABLE_SOCIAL = 'prd_mega.sprocu92.social_enterprise_contract_list'
SOCIAL_CONTRACT_CRITERIA = 'prd_mega.sprocu92.social_enterprise_contract_criteria'

file_name = os.path.join("..",'auxiliary', 'social_enterprise_cpv_mapping.json')
# Create a filtered list of the contracts which match the predefined list of CPVs 
create_contract_list(file_name, LIST_TABLE_SOCIAL)

#Parse the criteria for the filtered list of criterias for Romania Social Protection Project
create_contract_criteria_table(LIST_TABLE_SOCIAL, SOCIAL_CONTRACT_CRITERIA)

# COMMAND ----------

# MAGIC %md
# MAGIC Create Criteria Table for Infra Project 

# COMMAND ----------

LIST_TABLE_INFRA = 'prd_mega.sprocu92.infra_contract_list'
INFRA_CONTRACT_CRITERIA = 'prd_mega.sprocu92.infra_contract_criteria'
file_name = os.path.join('auxiliary', 'infra_cpv_mapping.json')

# Create a filtered list of the contracts which match the predefined list of CPVs 
create_contract_list(file_name, LIST_TABLE_INFRA)

#Parse the criteria for the filtered list of criterias for Romania Social Protection Project
create_contract_criteria_table(LIST_TABLE_INFRA, INFRA_CONTRACT_CRITERIA)

# COMMAND ----------



# COMMAND ----------

df_list = spark.table(LIST_TABLE_SOCIAL).toPandas()
df = spark.table(SOCIAL_CONTRACT_CRITERIA).toPandas()
category1 = df_list[(df_list.category == 'Electrical and electronic equipment used in the healthcare sector') & (df_list.status == 5) ].cNoticeId.astype(str)
category2  = df_list[(df_list.category == 'Food') & (df_list.status == 5) ].cNoticeId.astype(str)
category3  = df_list[(df_list.category == 'Furniture') & (df_list.status == 5) ].cNoticeId.astype(str)

# COMMAND ----------

criteria_df1 = df[df.documentId.isin(category1.values)]
criteria_df2 = df[df.documentId.isin(category2)]
criteria_df3 = df[df.documentId.isin(category3)]

# COMMAND ----------

sampled_ids = criteria_df1.documentId.drop_duplicates().sample(n=30, random_state=42).tolist()
criteria_df1 = criteria_df1[criteria_df1.documentId.isin(sampled_ids)]

sampled_ids = criteria_df2.documentId.drop_duplicates().sample(n=30, random_state=42).tolist()
criteria_df2 = criteria_df2[criteria_df2.documentId.isin(sampled_ids)]

sampled_ids = criteria_df3.documentId.drop_duplicates().sample(n=30, random_state=42).tolist()
criteria_df3 = criteria_df3[criteria_df3.documentId.isin(sampled_ids)]


# COMMAND ----------

SAMPLE_FOLDR = '/Volumes/prd_mega/sprocu92/vprocu92/Documents/sample/'
criteria_df1.to_csv(SAMPLE_FOLDR + 'electrical.csv', index=False)
criteria_df2.to_csv(SAMPLE_FOLDR + 'food_criteria.csv', index=False)
criteria_df3.to_csv(SAMPLE_FOLDR + 'furniture.csv', index=False)

# COMMAND ----------

criteria_df2

# COMMAND ----------


