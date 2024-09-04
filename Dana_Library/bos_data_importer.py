# Dana_Library/bos_data_importer.py
import requests
import pandas as pd

def bos_data_api_import(resource_id):
    url = f"https://data.boston.gov/api/3/action/datastore_search?resource_id={resource_id}"
    limit = 1000  # Number of records per request
    offset = 0
    all_records = []

    while True:
        # Fetch a batch of records
        response = requests.get(url, params={'limit': limit, 'offset': offset})
        if response.status_code == 200:
            data = response.json()
            records = data['result']['records']
            all_records.extend(records)
            
            # Check if there are more records to fetch
            if len(records) < limit:
                break
            
            # Move to the next batch
            offset += limit
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
            break

    # Convert all records to a DataFrame
    df = pd.DataFrame(all_records)
    return df