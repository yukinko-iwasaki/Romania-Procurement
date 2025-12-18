# Databricks notebook source
DEST_ALL_TABLE = 'prd_mega.sprocu92.contract_bronze'
LIST_TABLE = 'prd_mega.sprocu92.contract_list'
AWARDED = 'Atribuita'
CONTRACT_LIST_SILVER = 'prd_mega.sprocu92.contract_list_silver'

# COMMAND ----------

from pyspark.sql.functions import col


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

contract_list = contract_list[contract_list['sysProcedureStateText'] == AWARDED]


contract_df = spark.read.table(DEST_ALL_TABLE).toPandas()
contract_df = contract_df[contract_df.documentId.isin(contract_list.cNoticeId.values)]

# COMMAND ----------

import json

# def contains_keywords(row, keywords):
#     value = json.loads(row).get('personalSituation', '')
#     # return True only if *all* keywords are present
#     return all(keyword in value for keyword in keywords)
def is_social(row, col):
    value = json.loads(row).get(col, '')
    # return True only if *all* keywords are present
    return value

# # apply the check to the section3 column
# contract_df['is_social_contract'] = contract_df.section3.apply(
#     lambda r: contains_keywords(r, ['98/2016', '56'])
# )

# apply the check to the section3 column
contract_df['restrictedToShelterdWorkshop'] = contract_df.section3.apply(
    lambda r: is_social(r, 'restrictedToShelterdWorkshop')
)
# apply the check to the section3 column
contract_df['isReservedContract'] = contract_df.section3.apply(
    lambda r: is_social(r, 'isReservedContract')
)

# apply the check to the section3 column
contract_df['restrictedToPrograms'] = contract_df.section3.apply(
    lambda r: is_social(r, 'restrictedToPrograms')
)

# COMMAND ----------

contract_list = contract_list.merge(contract_df[['documentId', 'restrictedToShelterdWorkshop', 'isReservedContract', 'restrictedToPrograms', 'contractTitle', 'contractingAuthorityNameAndFN']], left_on='cNoticeId', right_on='documentId', how='inner').drop(columns=['documentId'])


# COMMAND ----------

spark_df = spark.createDataFrame(contract_list)
spark_df.write.mode("overwrite").saveAsTable(CONTRACT_LIST_SILVER)

# COMMAND ----------

df = spark.table(CONTRACT_LIST_SILVER)

# COMMAND ----------

df.columns

# COMMAND ----------

contract_list = df.select(['cNoticeId', 'restrictedToShelterdWorkshop', 'restrictedToPrograms']).toPandas()


# COMMAND ----------

reserved = contract_list[contract_list['restrictedToShelterdWorkshop'] | contract_list['restrictedToShelterdWorkshop']]

# COMMAND ----------

reserved['URL'] = reserved.cNoticeId.apply(lambda x: f'https://www.e-licitatie.ro/pub/notices/c-notice/v2/view/{x}')

# COMMAND ----------

reserved.to_csv('./reserved.csv')

# COMMAND ----------

len(reserved)

# COMMAND ----------

len(contract_list)

# COMMAND ----------


