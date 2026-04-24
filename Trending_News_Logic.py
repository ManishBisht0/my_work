#################################### Trending-News ###############################
from azure.cosmos import CosmosClient, PartitionKey
from datetime import datetime, timedelta, timezone
import pandas as pd
import pytz
import json
import warnings
import importlib
import not_an_org
 
warnings.filterwarnings('ignore')
importlib.reload(not_an_org)
from not_an_org import lst_not_org
 
endpoint = 'https://azure.cosmosdb'
key = "please insert your key here "
 
def query_to_cosmos(endpoint, key, database_name, container_name, query):
    client = CosmosClient(endpoint, key)
    db = client.get_database_client(database_name)
    container = db.get_container_client(container_name)
    items = list(container.query_items(query, enable_cross_partition_query=True))
    df = pd.DataFrame(items)
    return df
 
ist_timezone = pytz.timezone('Asia/Kolkata')
current_time_in_ist = datetime.now(ist_timezone)
today_date = '2024-08-14'
print(today_date)
 
org_query = f"""SELECT c.id, c.Art_Id, c.ORG, c.published_date, c.published_date_time 
                FROM c 
                WHERE IS_DEFINED(c.ORG) AND ARRAY_LENGTH(c.ORG) > 0 
                AND CONTAINS(c.published_date, '{today_date}')"""
 
df1 = query_to_cosmos(endpoint, key, 'cdb-L1', 'ORG', org_query)
 
################################# Org based news #################################
df1.drop_duplicates(subset='Art_Id', inplace=True)
df1.reset_index(drop=True, inplace=True)
updated_df = df1.explode('ORG')
updated_df['ORG'] = updated_df['ORG'].str.strip().str.replace('"', ' ').str.lower()
 
######################### From published date-time ##################################
updated_df['combined_datetime'] = pd.to_datetime(updated_df['published_date'] + ' ' + updated_df['published_date_time'])
updated_df['date'] = updated_df['combined_datetime'].dt.strftime('%Y-%m-%d')
updated_df['hour'] = updated_df['combined_datetime'].dt.strftime('%H:%M:%S')
 
grouped_df = updated_df.groupby(['date', 'hour', 'ORG', 'id', 'Art_Id']).size().reset_index(name='Frequency')
 
########################### Creating Time Intervals ############################
empty_df = pd.DataFrame()
hours_range = range(24)
 
for current_hour in hours_range:
    for current_date in grouped_df['date'].unique():
        start_hour = current_hour
        end_hour = (current_hour + 1) % 24
        start_time = pd.to_datetime(f'{start_hour:02}:00:00', format='%H:%M:%S').time()
        end_time = pd.to_datetime(f'{end_hour:02}:00:00', format='%H:%M:%S').time()
 
        grouped_df['hour'] = pd.to_datetime(grouped_df['hour'], format='%H:%M:%S').dt.time
 
        interval_dataframe = grouped_df[(grouped_df['hour'] >= start_time) &
                                        (grouped_df['hour'] < end_time) &
                                        (grouped_df['date'] == current_date)]
 
        interval_counts = interval_dataframe.groupby('ORG').agg({
            'Art_Id': lambda x: x.tolist(),
            'Frequency': 'sum'
        }).reset_index()
 
        interval_counts['Time_Interval'] = f'{start_hour:02}:00 - {end_hour:02}:00'
        interval_counts['Date'] = current_date
 
        empty_df = pd.concat([empty_df, interval_counts], ignore_index=True)
sorted_result = empty_df[empty_df['Frequency'] >= 1]
sorted_result = sorted_result.sort_values(by=['Date', 'Time_Interval', 'Frequency'], ascending=[False, False, False])
 
############################### Separating Not Org Elements ###################################
sorted_result = sorted_result[~sorted_result['ORG'].str.lower().isin(map(str.lower, lst_not_org))].reset_index(drop=True)
 
###################### Function to Convert to Epoch Time #########################
def convert_to_epoch(date, time_range, time_offset):
    start_time = time_range.split(' - ')[0]  # Get the start time
    date_time_str = f"{date} {start_time}:00"
    local_time = datetime.strptime(date_time_str, "%Y-%m-%d %H:%M:%S")
    utc_time = local_time - time_offset  # Adjust for time zone offset
    epoch_time = int(utc_time.replace(tzinfo=timezone.utc).timestamp())
    return epoch_time
 
# Assuming IST (UTC+5:30) as the local time
time_offset = timedelta(hours=5, minutes=30)
 
sorted_result['Epoch'] = sorted_result.apply(lambda row: convert_to_epoch(row['Date'], row['Time_Interval'], time_offset), axis=1)
 
########################### Grouping and Final Output ###########################
grouped = sorted_result.groupby(['Epoch'])
 
result = []
date_final = str(today_date)
for epoch, group in grouped:
    top_entries = group.sort_values(by='Frequency', ascending=False).head(5)
    trending_entry = {
        "time_interval": top_entries.iloc[0]['Time_Interval'],
        "Epoch": epoch[0],  # Ensure epoch is an integer
        "Org_Trending": []
    }
    for _, row in top_entries.iterrows():
        org_entry = {
            "Company_Name": row['ORG'],
            "Art_Id": row['Art_Id'],
            "Frequency": row['Frequency']
        }
        trending_entry["Org_Trending"].append(org_entry)
    result.append(trending_entry)
final_output = {
    "Published_Date": date_final,
    "Trending": result
}
 
print(json.dumps(final_output, indent=4))

import uuid
from azure.cosmos import CosmosClient
from azure.cosmos.exceptions import CosmosHttpResponseError
 
def upsert_or_insert_data(endpoint, key, database_name, container_name, data):
    client = CosmosClient(endpoint, key)  # Create the client once
    db = client.get_database_client(database_name)
    container = db.get_container_client(container_name)
 
    # Ensure there's only one item to process
    if not data or len(data) > 1:
        print("Error: Expected exactly one entry in data")
        return
 
    item = data[0]
    item_id = str(uuid.uuid4())  # Unique ID for each item
    published_date = item.get('Published_Date')
    trending_data = item.get('Trending')
 
    new_item = {
        'id': item_id,  # UUID as the unique ID
        'published_date': published_date,
        'Trending': trending_data
    }
 
    try:
        # Query to check if an item with the same published_date exists
        query = f"SELECT * FROM c WHERE c.published_date = '{published_date}'"
        existing_items = list(container.query_items(query, enable_cross_partition_query=True))
 
        if existing_items:
            # If item exists, update the existing item
            existing_item = existing_items[0]
            existing_item['Trending'] = trending_data
            container.upsert_item(existing_item)
            print(f"Upserted item with published_date: {published_date}")
        else:
            # If item does not exist, create a new item
            container.create_item(new_item)
            print(f"Inserted new item with published_date: {published_date}")
 
    except CosmosHttpResponseError as e:
        print(f"Error upserting/inserting item: {e}")
data_to_upsert_or_insert = [{
    'Published_Date': final_output['Published_Date'],
    'Trending': final_output['Trending']
}]
 
upsert_or_insert_data(endpoint, key, 'database-name', 'container-name', data_to_upsert_or_insert)
