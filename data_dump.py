import pymongo
import pandas as pd
import json


client = pymongo.MongoClient("mongodb+srv://prabhat2:Pr171219@cluster0.dlylezz.mongodb.net/?retryWrites=true&w=majority")

DATA_FILE_PATH= "/config/workspace/energy-efficiency-data-set.csv"
DATABASE_NAME = "energy"
COLLECTION_NAME= "load"

if __name__== "__main__":
    df= pd.read_csv(DATA_FILE_PATH)
    print(f"Rows and columns: {df.shape}")

    #Convert dataframe to json so that we can dump these record in mongo db
    df.reset_index(drop=True, inplace=True)
    json_record = list(json.loads(df.T.to_json()).values())
    print(json_record[0])

    #insert converted json record to mongo db
    client[DATABASE_NAME][COLLECTION_NAME].insert_many(json_record) 