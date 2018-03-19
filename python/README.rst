Phoenix database adapter for Python
===================================

.. image:: https://code.oxygene.sk/lukas/python-phoenixdb/badges/master/pipeline.svg
    :target: https://code.oxygene.sk/lukas/python-phoenixdb/commits/master
    :alt: Build Status

.. image:: https://readthedocs.org/projects/python-phoenixdb/badge/?version=latest
    :target: http://python-phoenixdb.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

``phoenixdb`` is a Python library for accessing the
`Phoenix SQL database <http://phoenix.apache.org/>`_
using the
`remote query server <http://phoenix.apache.org/server.html>`_.
The library implements the
standard `DB API 2.0 <https://www.python.org/dev/peps/pep-0249/>`_ interface,
which should be familiar to most Python programmers.

Installation
------------

The easiest way to install the library is using `pip <https://pip.pypa.io/en/stable/>`_::

    pip install phoenixdb

You can also download the source code from `here <https://phoenix.apache.org/download.html>`_,
extract the archive and then install it manually::

    cd /path/to/apache-phoenix-x.y.z/phoenix
    python setup.py install

Usage
-----

The library implements the standard DB API 2.0 interface, so it can be
used the same way you would use any other SQL database from Python, for example::

    import phoenixdb
    import phoenixdb.cursor

    database_url = 'http://localhost:8765/'
    conn = phoenixdb.connect(database_url, autocommit=True)

    cursor = conn.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, username VARCHAR)")
    cursor.execute("UPSERT INTO users VALUES (?, ?)", (1, 'admin'))
    cursor.execute("SELECT * FROM users")
    print(cursor.fetchall())

    cursor = conn.cursor(cursor_factory=phoenixdb.cursor.DictCursor)
    cursor.execute("SELECT * FROM users WHERE id=1")
    print(cursor.fetchone()['USERNAME'])


Setting up a development environment
------------------------------------

If you want to quickly try out the included examples, you can set up a
local `virtualenv <https://virtualenv.pypa.io/en/latest/>`_ with all the
necessary requirements::

    virtualenv e
    source e/bin/activate
    pip install -r requirements.txt
    python setup.py develop

To create or update the Avatica protobuf classes, change the tag in ``gen-protobuf.sh``
and run the script.

If you need a Phoenix query server for experimenting, you can get one running
quickly using `Docker <https://www.docker.com/>`_::

    docker-compose up

Or if you need an older version of Phoenix::

    PHOENIX_VERSION=4.9 docker-compose up

If you want to use the library without installing the phoenixdb library, you can use
the `PYTHONPATH` environment variable to point to the library directly::

    cd $PHOENIX_HOME/python
    python setup.py build
    cd ~/my_project
    PYTHONPATH=$PHOENIX_HOME/build/lib python my_app.py

Interactive SQL shell
---------------------

There is a Python-based interactive shell include in the examples folder, which can be
used to connect to Phoenix and execute queries::

    ./examples/shell.py http://localhost:8765/
    db=> CREATE TABLE test (id INTEGER PRIMARY KEY, name VARCHAR);
    no rows affected (1.363 seconds)
    db=> UPSERT INTO test (id, name) VALUES (1, 'Lukas');
    1 row affected (0.004 seconds)
    db=> SELECT * FROM test;
    +------+-------+
    |   ID | NAME  |
    +======+=======+
    |    1 | Lukas |
    +------+-------+
    1 row selected (0.019 seconds)

Running the test suite
----------------------

The library comes with a test suite for testing Python DB API 2.0 compliance and
various Phoenix-specific features. In order to run the test suite, you need a
working Phoenix database and set the ``PHOENIXDB_TEST_DB_URL`` environment variable::

    export PHOENIXDB_TEST_DB_URL='http://localhost:8765/'
    nosetests

Similarly, tox can be used to run the test suite against multiple Python versions::

    pyenv install 3.5.5
    pyenv install 3.6.4
    pyenv install 2.7.14
    pyenv global 2.7.14 3.5.5 3.6.4
    PHOENIXDB_TEST_DB_URL='http://localhost:8765' tox

Known issues
------------

- You can only use the library in autocommit mode. The native Java Phoenix library also implements batched upserts, which can be committed at once, but this is not exposed over the remote server. This was previously unimplemented due to CALCITE-767, however this functionality exists in the server, but is lacking in the driver.
  (`CALCITE-767 <https://issues.apache.org/jira/browse/CALCITE-767>`_)
- TIME and DATE columns in Phoenix are stored as full timestamps with a millisecond accuracy,
  but the remote protocol only exposes the time (hour/minute/second) or date (year/month/day)
  parts of the columns. (`CALCITE-797 <https://issues.apache.org/jira/browse/CALCITE-797>`_, `CALCITE-798 <https://issues.apache.org/jira/browse/CALCITE-798>`_)
- TIMESTAMP columns in Phoenix are stored with a nanosecond accuracy, but the remote protocol truncates them to milliseconds. (`CALCITE-796 <https://issues.apache.org/jira/browse/CALCITE-796>`_)
- ARRAY columns are not supported. Again, this previously lacked server-side support which has since been built. The
  driver needs to be updated to support this functionality.
  (`CALCITE-1050 <https://issues.apache.org/jira/browse/CALCITE-1050>`_, `PHOENIX-2585 <https://issues.apache.org/jira/browse/PHOENIX-2585>`_)
