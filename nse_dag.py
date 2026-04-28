from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import requests
import json
import psycopg2
import pandas as pd
import numpy as np
from sqlalchemy import create_engine


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

def load_nifty_data(input_df):
    engine = create_engine(
    "postgresql+psycopg2://postgres:postgres@postgres:5432/postgres"
    )
    input_df.to_sql(
    "nifty_data",
    engine,
    if_exists="append",
    index=False
    )


def load_nifty_raw (input_data):
    """ Load Transformed data into Postgres SQL"""
        
    conn = psycopg2.connect(
        host="postgres",
        database="postgres",
        user="postgres",
        password="postgres",
        port=5432
        )

    cursor = conn.cursor()
    #cursor.execute("SELECT current_database();")
    #print(cursor.fetchone())

    
    cursor.execute("""
            CREATE TABLE IF NOT EXISTS nifty_raw (
        name TEXT,
        timestamp TEXT,
        advance JSONB,
        metadata JSONB,
        market_status JSONB
        );
                   """
            )
    cursor.execute("""
                   
            INSERT INTO nifty_raw (name, timestamp, advance, metadata, market_status)
            VALUES (%s,%s,%s,%s,%s)
            """, (
                input_data['name'],
                input_data['timestamp'],
                json.dumps(input_data['advance']),
                json.dumps(input_data['metadata']),
                json.dumps(input_data['marketStatus'])
                )

            )
    
    conn.commit()
    cursor.close()
    return "nifty 50 data input done"

def transform_data(output):
    df,time_of_request,advances,metadata,market_status=output
    nifty50 = {
    'name': "NIFTY 50",
    'advance': advances,
    'timestamp': time_of_request,
    'metadata': metadata,
    'marketStatus': market_status
    }
    
    df= pd.DataFrame(df)
    nifty_data=df[df["symbol"]=="NIFTY 50"]
    nifty_data.drop(columns=["priority",'series', 'meta','chartTodayPath','chart30dPath','chart365dPath','identifier'],
           inplace=True)
    #print(nifty_data)
    stocks_data=df[df["symbol"]!="NIFTY 50"]
    #stocks_data.head()
    meta_df=stocks_data["meta"]
    meta_df = pd.json_normalize(meta_df)
    #meta_df.head()
    stocks_data=stocks_data.drop(columns=["meta"])

    #return load_nifty_raw(nifty50)
    return load_nifty_data(nifty_data)




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
