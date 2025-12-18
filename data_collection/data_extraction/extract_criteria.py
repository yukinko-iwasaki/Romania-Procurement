# Databricks notebook source
import requests

# COMMAND ----------

DEST_TABLE = 'prd_mega.sprocu92.criteria'

# COMMAND ----------

# Define base URL and product groups
BASE_URL = "https://www.mvicriteria.nl/api/criteria"

# All product groups (1-51, excluding 2, 3, 16, 40)
PRODUCT_GROUPS = [
    1, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35,
    36, 37, 38, 39, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51
]

# Build query parameters
params = {
    'product_groups[]': PRODUCT_GROUPS,
    'language': 'en'
}

# Construct the full URL
import urllib.parse
query_string = urllib.parse.urlencode(params, doseq=True)
URL = f"{BASE_URL}?{query_string}"

print(f"Number of product groups: {len(PRODUCT_GROUPS)}")
print(f"URL: {URL}")

# COMMAND ----------

# Call the API and store the information
import requests
import json
import pandas as pd

try:
    # Make the API request
    response = requests.get(URL)
    response.raise_for_status()  # Raise an exception for bad status codes
    
    # Parse the JSON response
    data = response.content


    # Save to JSON
    output_file = "data/mvi_criterias.txt"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(data.decode('utf-8'))
    print(f"Data saved to: {output_file}")
except requests.exceptions.RequestException as e:
    print(f"Error making API request: {e}")
except json.JSONDecodeError as e:
    print(f"Error parsing JSON response: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
