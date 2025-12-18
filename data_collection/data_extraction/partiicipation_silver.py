# Databricks notebook source
PARTICIPANTS_BRONZE = 'prd_mega.sprocu92.participants_list_bronze'
PARTICIPANTS_SILVER = 'prd_mega.sprocu92.participants_list_silver'
AWARD_BRONZE = 'prd_mega.sprocu92.award_notice_bronze'

from pyspark.sql.types import *
import pandas as pd
import json

# COMMAND ----------

participation_df = spark.read.table(PARTICIPANTS_BRONZE).toPandas()

results = []
for index, row in participation_df.iterrows():
    documentId = row.documentId

    partitipation_docs = json.loads(row.participation)
    maxVersion = 1
    temp = {}
    for doc in partitipation_docs:
        if doc['versionNo'] >= maxVersion:
            maxVersion = doc['versionNo']
            temp = doc
        else:
            continue
    temp['documentId'] = documentId
    results.append(temp)
participations = pd.DataFrame(results)


# COMMAND ----------

results = []
for index, row in participations.iterrows():
    suppliers = row.statementSuppliers
    for supplier in suppliers:
        for participant in supplier['statementParticipants']:
            participant['documnetId']= row.documentId
            results.append(participant)
supplier_df = pd.DataFrame(results)
# Extract only the numeric part of the fiscalNumber column
supplier_df['fiscalNumber_numeric'] = supplier_df['fiscalNumber'].str.replace(r'\D+', '', regex=True)
    

# COMMAND ----------

social_enterprise = pd.read_csv('./Social enterprises-august-2025(RUEIS).csv')
social_fiscal = social_enterprise["4"].dropna()
supplier_df['is_socail_enterprise'] = supplier_df['fiscalNumber_numeric'].isin(social_fiscal.values)

# COMMAND ----------

schema = StructType([
    StructField('statementSupplierId', StringType(), True),
    StructField('statementParticipantId', StringType(), True),
    StructField('sysProcedureSupplierClass', StringType(), True),
    StructField('contractPercent', DoubleType(), True),
    StructField('isLead', BooleanType(), True),
    StructField('fiscalNumber', StringType(), True),
    StructField('name', StringType(), True),
    StructField('tradeRegisterNumber', StringType(), True),
    StructField('representativeName', StringType(), True),
    StructField('sysSupplierCategory', StringType(), True),
    StructField('isGenerated', BooleanType(), True),
    StructField('entityId', StringType(), True),
    StructField('validationErrors', StringType(), True),
    StructField('documnetId', StringType(), True),
    StructField('fiscalNumber_numeric', StringType(), True),
    StructField('is_socail_enterprise', BooleanType(), True)
])

# COMMAND ----------

df = spark.createDataFrame(supplier_df, schema=schema)
df.write.mode("overwrite").saveAsTable(PARTICIPANTS_SILVER)

# COMMAND ----------


