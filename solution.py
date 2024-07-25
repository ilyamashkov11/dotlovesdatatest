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

    # cleaning the data and saving to a new csv
    df_cleaned = cleanData(df)
    df_cleaned.to_csv("for-applicants/clean_sample.csv", index=False)

    # creating the db and a cursor which is used to interact with the db
    connection = sqlite3.connect("for-applicants/outlet_transactions.db")

    # creating the table called "outlet_transactions" with an index column
    df_cleaned.to_sql("for-applicants/outlet_transactions", connection, if_exists="replace", index=True)
    
    connection.close()


def cleanData(df):
    # converting n_trans column to ints as some are doubles. It should only be whole numbers anyway
    df["N_TRANS"] = df["N_TRANS"].astype(int)
    try:
        return pd.to_datetime(df["DATE"], format='%Y-%m-%d')
    except ValueError:
        indexesToDrop = []
        for index, date in enumerate(df["DATE"]):
            dateArray = date.split("-")
            val1, val2, val3 = dateArray
            
            # if the first value isnt a year means bad data
            if len(val1) <= 2:
                #if both month and day are < 12 then its impossible to tell which one is which
                if (int(val1) <= 12 and int(val2)) <= 12:
                    indexesToDrop.append(index)
                # if val1 > 12 that means its the day
                elif int(val1) > 12:
                    dateArray = [val3, val2, val1]
                    date = "-".join(dateArray)
                    df.at[index, "DATE"] = date

                # if val2 > 12 that means its the day 
                elif int(val2) > 12:
                    dateArray = [val3, val1, val2]
                    date = "-".join(dateArray)
                    df.at[index, "DATE"] = date

        df = df.drop(indexesToDrop)
        return df
                    

# counts invalid dates that dont follow the year-month-day format
def is_valid_date(date):
    try:
        pd.to_datetime(date, format='%Y-%m-%d')
        return False
    except ValueError:
        return True

loadData()