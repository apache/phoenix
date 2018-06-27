import phoenixdb
import phoenixdb.cursor

database_url = 'http://localhost:8765/'

print "CREATING PQS CONNECTION"
conn = phoenixdb.connect(database_url, autocommit=True, auth="SPNEGO")
cursor = conn.cursor()

cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, username VARCHAR)")
cursor.execute("UPSERT INTO users VALUES (?, ?)", (1, 'admin'))
cursor.execute("SELECT * FROM users")
print cursor.fetchall()