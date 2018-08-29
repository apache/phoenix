<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Apache Phoenix Python Driver "PhoenixDB"

This directory contains the Python driver for Apache Phoenix called "Python PhoenixDB" or just "PhoenixDB".

This driver implements the Python DB 2.0 API for database drivers as described by [PEP-249](https://www.python.org/dev/peps/pep-0249/).
This driver is implemented using the Phoenix Query Server (PQS) and the [Apache Calcite
Avatica](https://calcite.apache.org/avatica) project.

This driver should be compatible with Python 2.7 and Python 3.3+ and support both unauthenticated access and
authenticated access via SPNEGO to PQS.

## Usage

The use of a virtual Python environment is strongly recommended, e.g. [virtualenv](https://virtualenv.pypa.io/en/stable/) or [conda](https://conda.io/docs/). You can install one of these using the Python package manager [pip](https://pypi.org/project/pip/). For developers who need to support multiple versions of Python, Python version managers, such as [pyenv](https://github.com/pyenv/pyenv), can drastically improve your quality of life.

When connecting to an unsecured PQS instance, install the phoenixdb module into your local environment and write your
application.

```bash
$ virtualenv e
$ source e/bin/activate
$ pip install file:///path/to/phoenix-x.y.z/phoenix/python/phoenixdb
$ cat <<EOF
import phoenixdb
import phoenixdb.cursor

if __name__ == '__main__':
  database_url = 'http://localhost:8765/'
  conn = phoenixdb.connect(database_url, autocommit=True)
  cursor = conn.cursor()
  cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, username VARCHAR)")
  cursor.execute("UPSERT INTO users VALUES (?, ?)", (1, 'admin'))
  cursor.execute("SELECT * FROM users")
  print(cursor.fetchall())
EOF > test-client.py
$ python test-client.py
```

When using a PQS instance secured via SPNEGO with Kerberos-based authentication, you must also install the custom
release of requests-kerberos provided with PhoenixDB.


```bash
$ virtualenv e
$ source e/bin/activate
$ pip install file:///path/to/phoenix-x.y.z/phoenix/python/phoenixdb
$ pip install file:///path/to/phoenix-x.y.z/phoenix/python/requests-kerberos
$ cat <<EOF
import phoenixdb
import phoenixdb.cursor

if __name__ == '__main__':
  database_url = 'http://localhost:8765/'
  conn = phoenixdb.connect(database_url, autocommit=True, auth="SPNEGO")
  cursor = conn.cursor()
  cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, username VARCHAR)")
  cursor.execute("UPSERT INTO users VALUES (?, ?)", (1, 'admin'))
  cursor.execute("SELECT * FROM users")
  print(cursor.fetchall())
EOF > test-client.py
$ python test-client.py
```

Please see the README included with PhoenixDB for more information on using the Python driver.

## Kerberos support in testing

An integration test, `SecureQueryServerPhoenixDBIT`, is included with Phoenix that sets up a secured Phoenix installation with PQS, then
uses the driver to interact with that installation. We have observed that, with a correct krb5.conf for
the Kerberos installation (MIT or Heimdal), this driver and the patched requests-kerberos library can
communicate with the secured PQS instance.

This test will guess at the flavor of Kerberos that you have installed on your local system. There is an option
exposed which will force a specific flavor to be assumed: `PHOENIXDB_KDC_IMPL`. Valid options are `MIT` and `HEIMDAL`.
You specify this as a Java system property from Maven, e.g. `mvn verify -Dtest=foo -Did.test=SecureQueryServerPhoenixDBIT -DPHOENIXDB_KDC_IMPL=MIT`
forces an MIT Kerberos style krb5.conf to be used for the test.
