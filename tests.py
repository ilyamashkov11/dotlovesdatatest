import solution
import sqlite3

# testing that the database gets created correctly by seeing if the first entry matches whats in the csv
def test1():
    print("test 1")
    connection = sqlite3.connect("outlet_transactions.db")
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM outlet_transactions LIMIT 1")
    print(cursor.fetchone())

test1()