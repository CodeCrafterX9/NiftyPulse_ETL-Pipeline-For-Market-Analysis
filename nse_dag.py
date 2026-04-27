from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import requests
import json
import psycopg2
import pandas as pd
import numpy as np

# THE OBJECTIVE OF THIS DAG IS TO 
# LOAD DATA FROM NSE WEBSITE DAILY - & STORE IT IN POSTGRES DB

def extract_data() :
    #Write a function to fetch data from NSE Stocks Wesite for todays' top 50 stocks
    session = requests.Session()
    url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/"
    }
    session.headers.update(headers)
    session.get("https://www.nseindia.com")
    response = session.get(url)
    data = response.json()
    #data is now in dictionary format 
    #dict_keys(['name', 'advance', 'timestamp', 'data', 'metadata', 'marketStatus'])
    df = data["data"]
    time_of_request = data["timestamp"]
    advances = data["advance"]
    metadata = data["metadata"]
    market_status= data["marketStatus"]
    #print(df.head(),advances,metadata,market_status)
    #note: dag() file ka print() logs mei show hota hai & return() XCom mei show hota hai
    return (df,time_of_request,advances,metadata,market_status)

def transform_data(output):
    df,time_of_request,advances,metadata,market_status=output
    nifty50 = pd.DataFrame({
    'name': "NIFTY 50",
    'advance': advances,
    'timestamp': time_of_request,
    'metadata': metadata,
    'marketStatus': market_status
    })
    
    df= pd.DataFrame(df)
    nifty_data=df[df["symbol"]=="NIFTY 50"]
    #print(nifty_data)
    stocks_data=df[df["symbol"]!="NIFTY 50"]
    #stocks_data.head()
    meta_df=stocks_data["meta"]
    meta_df = pd.json_normalize(meta_df)
    #meta_df.head()
    stocks_data=stocks_data.drop(columns=["meta"])

    return nifty_data , stocks_data , meta_df , stocks_data

default_args = {'owner':'airflow','start_date':datetime(2026,1,1)}

with DAG (dag_id="nse_stocks_etl", 
          default_args=default_args,
          schedule = '@daily',
          catchup = False) as dags :

    @task()
    def extract():
        return extract_data()

    @task
    def transform(output):
        return transform_data(output)
    
    output=extract()
    transform(output)
