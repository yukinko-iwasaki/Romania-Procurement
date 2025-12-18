# Databricks notebook source
import pandas as pd

INFRA_CONTRACT_CRITERIA = 'prd_mega.sprocu92.infra_contract_criteria'
LIST_TABLE_INFRA = 'prd_mega.sprocu92.infra_contract_list'

infra_criterias = spark.table(INFRA_CONTRACT_CRITERIA).toPandas()
infra_criteria_list = spark.table(LIST_TABLE_INFRA).toPandas()


# COMMAND ----------


infra_criteria_list = infra_criteria_list.copy()
infra_criteria_list['cNoticeId'] = infra_criteria_list['cNoticeId'].astype(str)

energy_list = infra_criteria_list[infra_criteria_list['category'] == 'Petroleum products, fuel, electricity and other sources of energy']
energy_criterias = infra_criterias.merge(energy_list, left_on='documentId', right_on='cNoticeId')

# COMMAND ----------

import json
green_procurement_file = '/Volumes/prd_mega/sprocu92/vprocu92/Documents/criteria/mvi_criteria.txt'
with open(green_procurement_file, 'r', encoding='utf-8') as f:
        green_procurement = json.load(f)

green_criterias = green_procurement['criteria']
# Test with Energy Procurement Criteria
# EVI product ids are 11 and 12
mvi_energy_criteria = {}
for _, green_criteria in green_criterias.items():
    for item in green_criteria['product_group_subjects']:
        if item['product_group_id'] not in [11, 12]:
            continue
        criteria_id = green_criteria['id']
        mvi_energy_criteria[criteria_id] = green_criteria['text']


# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

def get_prompt(contract_criteria, mvi_criteria):
    prompt = "I am a procurement specialist. I have a list of model criteria for energy green procurement. I also have a criteria coming from an actual contract notice. I would like to know if the actual criteria is a match for one of the model criterias."
    prompt += 'The actual criteria is in Romanian. Please translate it to English.'
    prompt += 'Please return a JSON with the following keys:  "match", "criteria from actual contract in original Romanina", "criteria from actural contract translated in English.'
    prompt += 'the the value for the "match" should contain the id of the model criteria that matches the actual criteria'

    prompt += f"Model criteria: {mvi_energy_criteria}"
    prompt += f"Actual criteria: {energy_criteria}"
    return prompt

# COMMAND ----------




# COMMAND ----------

chat = Chat()
energy_criterias = energy_criterias[energy_criterias.noticeAwardCriteriaName == 'Energie verde'].algorithmDescription
results = []
for energy_criteria in energy_criterias:
    prompt = get_prompt(energy_criteria, mvi_energy_criteria)
    message = chat.chat(prompt)
    message = message.choices[0].message.content[1]
    json_string = message['text'].removeprefix("```json").removesuffix("```").strip()
    results.append(json.loads(json_string))

# COMMAND ----------


results

# COMMAND ----------

mvi_energy_criteria[11619]

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

mvi_energy_criteria[11604]

# COMMAND ----------


