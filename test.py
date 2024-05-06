import sqlite3

# Connect to the SQLite database
connection = sqlite3.connect('timezones.db')
cursor = connection.cursor()

# Create a loop to continuously take SQL statements from the keyboard
while True:
    # Take input from the user
    table = input("Enter table name (or 'exit' to quit): ")

    # Check if the user wants to exit
    if table.lower() == 'exit':
        break

    sql_statement = 'SELECT * FROM ' + table

    try:
        # Execute the SQL statement
        cursor.execute(sql_statement)

        results = cursor.fetchall()
        for row in results:
            print(row)

    except sqlite3.Error as error:
        print("Error:", error)

# Close the cursor and connection
cursor.close()
connection.close()
