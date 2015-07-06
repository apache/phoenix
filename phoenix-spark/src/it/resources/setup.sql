-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE TABLE table1 (id BIGINT NOT NULL PRIMARY KEY, col1 VARCHAR)
CREATE TABLE table1_copy (id BIGINT NOT NULL PRIMARY KEY, col1 VARCHAR)
CREATE TABLE table2 (id BIGINT NOT NULL PRIMARY KEY, table1_id BIGINT, "t2col1" VARCHAR)
UPSERT INTO table1 (id, col1) VALUES (1, 'test_row_1')
UPSERT INTO table2 (id, table1_id, "t2col1") VALUES (1, 1, 'test_child_1')
UPSERT INTO table2 (id, table1_id, "t2col1") VALUES (2, 1, 'test_child_2')
UPSERT INTO table1 (id, col1) VALUES (2, 'test_row_2')
UPSERT INTO table2 (id, table1_id, "t2col1") VALUES (3, 2, 'test_child_1')
UPSERT INTO table2 (id, table1_id, "t2col1") VALUES (4, 2, 'test_child_2')
UPSERT INTO table2 (id, table1_id, "t2col1") VALUES (5, 2, 'test_child_3')
UPSERT INTO table2 (id, table1_id, "t2col1") VALUES (6, 2, 'test_child_4')
CREATE TABLE "table3" ("id" BIGINT NOT NULL PRIMARY KEY, "col1" VARCHAR)
UPSERT INTO "table3" ("id", "col1") VALUES (1, 'foo')
UPSERT INTO "table3" ("id", "col1") VALUES (2, 'bar')
CREATE TABLE ARRAY_TEST_TABLE (ID BIGINT NOT NULL PRIMARY KEY, VCARRAY VARCHAR[])
UPSERT INTO ARRAY_TEST_TABLE (ID, VCARRAY) VALUES (1, ARRAY['String1', 'String2', 'String3'])
CREATE TABLE DATE_PREDICATE_TEST_TABLE (ID BIGINT NOT NULL, TIMESERIES_KEY TIMESTAMP NOT NULL CONSTRAINT pk PRIMARY KEY (ID, TIMESERIES_KEY))
UPSERT INTO DATE_PREDICATE_TEST_TABLE (ID, TIMESERIES_KEY) VALUES (1, CAST(CURRENT_TIME() AS TIMESTAMP))
CREATE TABLE OUTPUT_TEST_TABLE (id BIGINT NOT NULL PRIMARY KEY, col1 VARCHAR, col2 INTEGER, col3 DATE)
CREATE TABLE CUSTOM_ENTITY."z02"(id BIGINT NOT NULL PRIMARY KEY)
UPSERT INTO CUSTOM_ENTITY."z02" (id) VALUES(1)