import phoenixdb
import phoenixdb.cursor
import sys


if __name__ == '__main__':
    pqs_port = sys.argv[1]
    database_url = 'http://localhost:' + str(pqs_port) + '/'

    print "CREATING PQS CONNECTION"
    conn = phoenixdb.connect(database_url, autocommit=True, auth="SPNEGO")
    cursor = conn.cursor()

    cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, username VARCHAR)")
    cursor.execute("UPSERT INTO users VALUES (?, ?)", (1, 'admin'))
    cursor.execute("UPSERT INTO users VALUES (?, ?)", (2, 'user'))
    cursor.execute("SELECT * FROM users")
    print "RESULTS"
    print cursor.fetchall()