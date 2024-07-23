import sqlite3
import pandas as pd

def loadData():
    # loading data from csv into a data frame
    df = pd.read_csv("for-applicants/sample.csv")

    # finding the sum of all rogue entries
    null_count= df.isnull().sum()
    invalid_shop_id_count = (df["SHOP_ID"] <= 0).sum()
    invalid_Ntrans_count = (df["N_TRANS"] < 0).sum()
    invalid_date_count = df["DATE"].apply(is_valid_date).sum()
    
    # displaying results to console
    print(null_count)
    print(invalid_shop_id_count)
    print(invalid_Ntrans_count)
    print(invalid_date_count)

    df_cleaned = cleanData(df["DATE"])
    # df_cleaned.to_csv("for-applicants/clean_sample.csv", index=False)

    # creating the db and a cursor which is used to interact with the db
    connection = sqlite3.connect("outlet_transactions.db")
    cursor = connection.cursor()

    # creating the table called "outlet_transactions" with an index column
    df.to_sql("outlet_transactions", connection, if_exists="replace", index=True)

    connection.close()

def cleanData(date):
    try:
        return pd.to_datetime(date, format='%Y-%m-%d')
    except ValueError:
        
    
    

# removes invalid dates that dont follow the year-month-day format
def is_valid_date(date):
    try:
        pd.to_datetime(date, format='%Y-%m-%d')
        return False
    except ValueError:
        return True
    

loadData()