

import requests   # pull data from api
import pandas as pd   #trandorm/read
from sqlalchemy import create_engine  #create connection to a db


#extract data from api

def extract() -> dict:
    API_URL = "http://universities.hipolabs.com/search?country=United+States"
    data = requests.get(API_URL).json()  #get a response as a json
    return data

def transform(data:dict) -> pd.DataFrame:
    df = pd.DataFrame(data)
    print(f"Total No.of Universities from API {len(data)}")

    #filter all california universities
    df = df[df["name"].str.contains("California")]
    print(f"No of Universities in California {len(df)}")

    #var list convert to string
    df['domains'] = [','.join(map(str, 1)) for l in df["domains"]]
    df['web_pages'] = [','.join(map(str, 1)) for l in df["web_pages"]]

    #drop index
    df = df.reset_index(drop=True)

    #return only desired column
    return df[["domains","country","web_pages","name"]]


def load(df:pd.DataFrame) -> None:
    disk_engine = create_engine('sqlite:///mydb.db')

    #save df to a table
    df.to_sql('cal_uni',disk_engine, if_exists='replace')

#execute the function

data = extract()
df = transform(data)
load(df)

