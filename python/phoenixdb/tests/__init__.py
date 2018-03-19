# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import unittest
import phoenixdb

TEST_DB_URL = os.environ.get('PHOENIXDB_TEST_DB_URL')


@unittest.skipIf(TEST_DB_URL is None, "these tests require the PHOENIXDB_TEST_DB_URL environment variable set to a clean database")
class DatabaseTestCase(unittest.TestCase):

    def setUp(self):
        self.conn = phoenixdb.connect(TEST_DB_URL, autocommit=True)
        self.cleanup_tables = []

    def tearDown(self):
        self.doCleanups()
        self.conn.close()

    def addTableCleanup(self, name):
        def dropTable():
            with self.conn.cursor() as cursor:
                cursor.execute("DROP TABLE IF EXISTS {}".format(name))
        self.addCleanup(dropTable)

    def createTable(self, name, columns):
        with self.conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS {}".format(name))
            cursor.execute("CREATE TABLE {} ({})".format(name, columns))
            self.addTableCleanup(name)
