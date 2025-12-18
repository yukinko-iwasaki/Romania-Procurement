# Databricks notebook source
# MAGIC %run ./utils

# COMMAND ----------

SOCIAL_CONTRACT_CRITERIA = 'prd_mega.sprocu92.social_contract_criteria'
SOCIAL_CRITERIAS = {
    "A. Social Return (employment of disadvantaged groups) ": "Clauses requiring or incentivising the contractor to employ people from disadvantaged / vulnerable groups (young people, Roma, disabled, etc.) or to create training/apprenticeship places, linked to the specific contract. Or giving priorities to social-economy actors or entities employing disadvantaged people.1 ",
    "B. Decent Work & Labour Conditions (core and supply chain)": "Clauses about respecting labour law, ILO conventions, transparent and predictable working conditions, health & safety, non-exploitative conditions – both in Romania and in global supply chains. ",
    "C. Accessibility & Design for All ": "Accessibility for persons with disabilities and older people in goods, services and works – and any “design for all” requirements.",
    "E. Equality, Diversity & Non-Discrimination ": "Measures promoting gender equality, equal pay, non-discrimination (race, ethnicity, disability, age, etc.), diversity in the workforce, work–life balance",
    "F. Human Rights & Ethical Trade / Supply-Chain Due Diligence (ISC-style) ": "Explicit “ethical trade”, “responsible sourcing” or “international social conditions” requirements: human rights, due diligence, risk mapping in global supply chains, especially high-risk sectors (textiles, electronics, agro-food, etc.). ",
}

def get_prompt(contract_criteria, social_criteria):
    prompt = "I am a world class procurement specialist. I have a list of model criteria for social procurement. I also have a criteria coming from an actual contract notice. I would like to know if the actual criteria is a match for one of the model criterias."
    prompt += 'The actual criteria is in Romanian. Please translate it to English.'
    prompt += 'Please return a JSON with the following keys:  "match", "criteria from actual contract in original Romanina", "criteria from actural contract translated in English.'
    prompt += 'the the value for the "match" should contain the id of the model criteria that matches the actual criteria'

    prompt += f"Model criteria: {social_criteria}"
    prompt += f"Actual criteria: {contract_criteria}"
    return prompt

# COMMAND ----------

criteria = spark.read.table(SOCIAL_CONTRACT_CRITERIA)
criteria = criteria.toPandas()
# criteria = criteria[criteria.noticeAwardCriteriaName.str.contains('social')] # this is for testing
# criteria = criteria[: 3000]

# COMMAND ----------

chat = Chat()
import json
import pandas as pd
results = []
for index, social_criteria in criteria.iterrows():
    description = social_criteria.noticeAwardCriteriaDescription
    name = social_criteria.noticeAwardCriteriaName
    detail = social_criteria.algorithmDescription
    prompt = get_prompt(f"{name}: {description}, {detail}", SOCIAL_CRITERIAS)
    message = chat.get_response(prompt=prompt)
    message = message.choices[0].message.content
    # json_string = message['text'].removeprefix("```json").removesuffix("```").strip()
    result = json.loads(message)
    result['id'] = social_criteria.documentId
    results.append(result)
    if (index+1) % 1000 == 0:
        print(index)
        pd.DataFrame(results).to_csv(f'./sample_{index}.csv')
        results = []

# COMMAND ----------

# MAGIC %md
# MAGIC Analysis Starts here on the extracted information
# MAGIC

# COMMAND ----------

criteria = spark.read.table(SOCIAL_CONTRACT_CRITERIA)
criteria = criteria.toPandas()

# COMMAND ----------

import pandas as pd
matched_criteria = pd.read_csv('./filtered_criteria_matched.csv')
# criteria_sample = criteria[:3000]

# criteria_sample['documentId'] = criteria_sample['documentId'].astype(str)
# matched_criteria['ID'] = matched_criteria['ID'].astype(str)

# COMMAND ----------

matched_criteria['URL'] = matched_criteria.ID.apply(lambda x: f"https://www.e-licitatie.ro/pub/notices/c-notice/v2/view/{x}")

# COMMAND ----------

matched_criteria.to_csv('./matched_criteria.csv')

# COMMAND ----------

CONTRACT_SAMPLE = 'prd_mega.sprocu92.criteria_sample3000'
spark_df = spark.createDataFrame(criteria_sample)
spark_df.write.mode("overwrite").saveAsTable(CONTRACT_SAMPLE)

# COMMAND ----------

MATCHED_CRITERIA = 'prd_mega.sprocu92.matched_criteria2'
spark_df = spark.createDataFrame(matched_criteria)
spark_df.write.mode("overwrite").saveAsTable(MATCHED_CRITERIA)

# COMMAND ----------

social_contracts = matched_criteria.ID.values

# COMMAND ----------

PARTICIPANTS_SILVER = 'prd_mega.sprocu92.participants_list_silver'


# COMMAND ----------

participations = spark.table(PARTICIPANTS_SILVER).toPandas()


# COMMAND ----------

participations['social_contract'] = participations.documnetId.isin(social_contracts)

# COMMAND ----------

participations_social = participations[participations.social_contract]
participations_non_social = participations[~participations.social_contract]

# COMMAND ----------

participations_social.is_socail_enterprise.value_counts()

# COMMAND ----------

import matplotlib.pyplot as plt

# Data
numbers = [
    5, 10, 10, 30, 20, 40, 30, 10, 30, 40,
    6, 10, 6, 25, 25, 10, 15, 5, 10, 30,
    30, 10, 30, 40, 30, 30, 20, 10, 5, 30,
    15, 30, 10, 15, 2, 30, 5, 5, 5, 5, 5
]

# Plot histogram with bin width of 10
fig, ax = plt.subplots(figsize=(8, 5))
ax.hist(numbers, bins=range(0, 101, 10), edgecolor="black")
ax.set_xlabel("noticeAwardCriteriaWeight")
ax.set_ylabel("Frequency")
ax.set_xlim(0, 100)
ax.set_title("Distribution of Numbers (On social)")

# COMMAND ----------



# COMMAND ----------

import pandas as pd
sample = pd.read_csv('./filtered_criteria.csv')

# COMMAND ----------

len(sample.ID.unique())

# COMMAND ----------

len(criteria.documentId.unique())

# COMMAND ----------

len(sample)

# COMMAND ----------


