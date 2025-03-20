import sqlite3

# Define database name
db_name = "Banks.db"

# Establish connection (creates the database if it does not exist)
sql_connection = sqlite3.connect(db_name)

# Create a cursor object to execute SQL queries
cursor = sql_connection.cursor()

# Create a table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS Largest_banks (
        Name TEXT,
        MC_USD_Billion REAL,
        MC_GBP_Billion REAL,
        MC_EUR_Billion REAL,
        MC_INR_Billion REAL
    )
''')

# Commit the changes and close the connection
sql_connection.commit()
sql_connection.close()

print(f"Database '{db_name}' and table 'Largest_banks' created successfully!")
