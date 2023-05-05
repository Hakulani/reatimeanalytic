import psycopg2 as p
import csv
import random
import time

## Config later ##
dbServerName    = "localhost"
dbUser          = "postgres-user"
dbPassword      = "postgres-pw"
dbName          = "customers"

conn   = p.connect(host=dbServerName, user=dbUser, password=dbPassword,
                                     dbname=dbName)

cursor = conn.cursor()

csv_file_path = 'food_coded.csv'
delimiter = ','
count = 1
with open(csv_file_path, 'r') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=delimiter)

    # Use the first row as the column names
    original_columns = next(csv_reader)
    columns = []

    # Add a unique identifier to each duplicate column name
    for col in original_columns:
        if col not in columns:
            columns.append(col)
        else:
            columns.append(f"{col}1")

    insert_row_sql = f"INSERT INTO foodcoded ({','.join(columns)}) VALUES ({','.join(['%s' for col in columns])})"
    for row in csv_reader:
        # Only insert values for the columns that exist in the table
        row_values = [row[original_columns.index(col)] if col in original_columns else None for col in columns]
        for i in range(len(row_values)):
            if row_values[i] == 'nan' or row_values[i]=='':
                row_values[i] = None
        cursor.execute(insert_row_sql, row_values)

        print('import row ',str(count))
        count = count+1
        conn.commit()
        time.sleep(random.randint(0, 2))

conn.commit()
cursor.close()
conn.close()

print(f"{len(columns)} columns and {cursor.rowcount} rows were imported from {csv_file_path} to foodcoded table.")