import phoenixdb
import phoenixdb.cursor

database_url = 'http://localhost:8765/'

print "CREATING PQS CONNECTION"
conn = phoenixdb.connect(database_url, autocommit=True, auth="SPNEGO")
cursor = conn.cursor()

print "CREATING TABLE"
cursor.execute("CREATE TABLE " + ${TABLE_NAME} + "(pk integer not null primary key)")
cursor.execute("UPSERT INTO " + ${TABLE_NAME} + " values(" + i + ")")
cursor.execute("SELECT * FROM " + ${TABLE_NAME})
print(cursor.fetchall())