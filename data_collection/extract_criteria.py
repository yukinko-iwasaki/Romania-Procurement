# Databricks notebook source
DEST_FOLDER = "data/"


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
    data = response.json()
    
    print(f"API call successful!")
    print(f"Response status code: {response.status_code}")
    print(f"Number of criteria returned: {len(data) if isinstance(data, list) else 'N/A'}")
    
    # Convert to DataFrame for easier analysis
    if isinstance(data, list):
        df = pd.DataFrame(data)
        print(f"DataFrame shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
    else:
        print("Response is not a list, storing as raw data")
        df = pd.json_normalize(data)
    
    # Save to CSV
    output_file = "data/mvi_criteria.csv"
    df.to_csv(output_file, index=False)
    print(f"Data saved to: {output_file}")
    
    # Display first few rows
    print("\nFirst 5 rows:")
    print(df.head())
    
except requests.exceptions.RequestException as e:
    print(f"Error making API request: {e}")
except json.JSONDecodeError as e:
    print(f"Error parsing JSON response: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")

# COMMAND ----------

results = []
for row in data['criteria']:
    results.append(data['criteria'][row])
pd.DataFrame(results).to_csv(f'{DEST_FOLDER}mvi_criteria.csv')

# COMMAND ----------

results = []
for row in data['subjects']:
    results.append(data['subjects'][row])
pd.DataFrame(results).to_csv(f'{DEST_FOLDER}/criteria_subjects.csv')

# COMMAND ----------

results = []
for row in data['tags']:
    results.append(data['tags'][row])
pd.DataFrame(results).to_csv(f'{DEST_FOLDER}/data/criteria_tags.csv')
