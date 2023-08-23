import pyarrow as pa
import sqlite3
import sys
import pandas as pd

# Provide the path to your .arrow metadata file

# Open the .arrow file
with pa.ipc.open_file(sys.argv[1]) as arrow_file:
    # df = pl.read_ipc(arrow_file)
    table = arrow_file.read_all()
    df = table.to_pandas()

# Give the table name and what accession number you want and badabing you've got the whole row
# Query syntax requires tuple so query, is a way to pass a single element tuple safely through SQL
def access_row_by_accession(table_name, query):
    cursor.execute(f"SELECT * FROM {table_name} WHERE Accession = ?", (query,))
    return cursor.fetchone()


# Accession number is always unique and will be indexed most often
df.set_index('Accession', inplace=True)

print(df.iloc[:, 0])

# where we'll be storing our database    
database_file = 'SQL_data.db'

# Connect, best to ensure that is is empty or has data in it
connection = sqlite3.connect(database_file)
# does not use pandas df
table = 'data_table'

df.to_sql(table, connection, index=True, if_exists='replace')

# This allows us to manipulate the SQL table
cursor = connection.cursor()

cursor.execute(f'PRAGMA table_info(data_table)')

columns = [row[1] for row in cursor.fetchall()]

print(columns)

print(access_row_by_accession(table, 'OP706894.1'))

connection.close()