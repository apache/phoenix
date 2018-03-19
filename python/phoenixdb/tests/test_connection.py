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

import unittest
import phoenixdb
from phoenixdb.tests import TEST_DB_URL


@unittest.skipIf(TEST_DB_URL is None, "these tests require the PHOENIXDB_TEST_DB_URL environment variable set to a clean database")
class PhoenixConnectionTest(unittest.TestCase):

    def _connect(self, connect_kw_args):
        try:
            r = phoenixdb.connect(TEST_DB_URL, **connect_kw_args)
        except AttributeError:
            self.fail("Failed to connect")
        return r

    def test_connection_credentials(self):
        connect_kw_args = {'user': 'SCOTT', 'password': 'TIGER', 'readonly': 'True'}
        con = self._connect(connect_kw_args)
        try:
            self.assertEqual(
                con._connection_args, {'user': 'SCOTT', 'password': 'TIGER'},
                'Should have extract user and password')
            self.assertEqual(
                con._filtered_args, {'readonly': 'True'},
                'Should have not extracted foo')
        finally:
            con.close()
