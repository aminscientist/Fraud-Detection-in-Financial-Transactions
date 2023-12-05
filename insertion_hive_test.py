from pyhive import hive

# Hive connection parameters
hive_host = "localhost"
hive_port = 10000

# Connect to Hive
conn = hive.Connection(host=hive_host, port=hive_port, username="root")

# Create a cursor
cursor = conn.cursor()

# Create a database
cursor.execute("CREATE DATABASE IF NOT EXISTS InertionTest")

# Use the database
cursor.execute("USE InertionTest")

# Create a table
cursor.execute("""
CREATE TABLE IF NOT EXISTS Test (
    id INT,
    name STRING,
    age INT
)
""")

# Insert data into the table
data_to_insert = [
    (1, 'Moaad', 27),
    (2, 'Mohamed', 20),
    (3, 'Hamza', 25)
]

for record in data_to_insert:
    cursor.execute(f"INSERT INTO Test VALUES ({record[0]}, '{record[1]}', {record[2]})")

# Commit the changes
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()
