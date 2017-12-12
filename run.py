import sqlite3
conn = sqlite3.connect('kernels.db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()
cursor.execute("SELECT * FROM Application;")

for row in cursor.fetchall():
    print(row["binary"],row["parameters"])

conn.close()
