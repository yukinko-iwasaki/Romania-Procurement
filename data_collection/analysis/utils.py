# Databricks notebook source
!pip install openai azure.identity pydantic


# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

schema = {
  "name": "ContractCriteria",
  "description": "Criteria extracted from a contract, including original Romanian and English translation.",
  "schema": { # <-- CRITICAL FIX: Use 'schema' instead of 'parameters'
    "type": "object",
    "properties": {
      "match": {
        "type": "string",
        "description": "The matching value or identifier for the criteria."
      },
      "criteria_from_actual_contract_in_original_romanian": {
        "type": "string",
        "description": "The specific contract criteria text in its original Romanian language."
      },
      "criteria_from_actual_contract_translated_in_english": {
        "type": "string",
        "description": "The specific contract criteria text translated into English."
      }
    },
    "required": [
      "match",
      "criteria_from_actual_contract_in_original_romanian",
      "criteria_from_actual_contract_translated_in_english"
    ],
    "additionalProperties": False
  }
}

# COMMAND ----------


import os
from pydantic import BaseModel, Field
from azure.identity import get_bearer_token_provider, ClientSecretCredential
from openai import AzureOpenAI

class ContractCriteria(BaseModel):
    # Use simple field names instead of alias
    match: str
    criteria_from_actual_contract_in_original_romanian: str
    criteria_from_actual_contract_translated_in_english: str


class Chat:
    def __init__(self):
        credential = ClientSecretCredential(
        tenant_id = dbutils.secrets.get(scope="DAPGPTKEYVAULT", key="GPT-APIM-Tenant-ID"),
        client_id = dbutils.secrets.get(scope="DAPGPTKEYVAULT", key="GPT-APIM-Client-ID"),
        client_secret = dbutils.secrets.get(scope="DAPGPTKEYVAULT", key="GPT-APIM-Client-Secret"))
        # set up Azure AD token provider
        token_provider = get_bearer_token_provider(credential, dbutils.secrets.get(scope="DAPGPTKEYVAULT", key="GPT-APIM-Token-Cred"))
        self.client = AzureOpenAI(
            azure_endpoint="https://azapim.worldbank.org/conversationalai/v2/openai/deployments/gpt-5/chat/completions?api-version=2025-01-01-preview",
            azure_ad_token_provider=token_provider,
            api_version="2025-04-01-preview",
        )

    def get_response(self, prompt, max_tokens=5000):
        response = self.client.chat.completions.create(
            model="gpt-5",
            messages=[
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            response_format={ "type": "json_schema", "json_schema": schema }
        )
        return response

# COMMAND ----------


