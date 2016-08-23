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
CREATE TABLE ARRAYBUFFER_TEST_TABLE (ID BIGINT NOT NULL PRIMARY KEY, VCARRAY VARCHAR[], INTARRAY INTEGER[])
UPSERT INTO ARRAYBUFFER_TEST_TABLE (ID, VCARRAY, INTARRAY) VALUES (1, ARRAY['String1', 'String2', 'String3'], ARRAY[1, 2, 3])
CREATE TABLE ARRAY_ANYVAL_TEST_TABLE (ID BIGINT NOT NULL PRIMARY KEY, INTARRAY INTEGER[], BIGINTARRAY BIGINT[])
UPSERT INTO ARRAY_ANYVAL_TEST_TABLE (ID, INTARRAY, BIGINTARRAY) VALUES (1, ARRAY[1, 2, 3], ARRAY[1, 2, 3])
CREATE TABLE ARRAY_BYTE_TEST_TABLE (ID BIGINT NOT NULL PRIMARY KEY, BYTEARRAY TINYINT[])
UPSERT INTO ARRAY_BYTE_TEST_TABLE (ID, BYTEARRAY) VALUES (1, ARRAY[1, 2, 3])
CREATE TABLE VARBINARY_TEST_TABLE (ID BIGINT NOT NULL PRIMARY KEY, BIN BINARY(1), VARBIN VARBINARY, BINARRAY BINARY(1)[])
CREATE TABLE DATE_PREDICATE_TEST_TABLE (ID BIGINT NOT NULL, TIMESERIES_KEY TIMESTAMP NOT NULL CONSTRAINT pk PRIMARY KEY (ID, TIMESERIES_KEY))
UPSERT INTO DATE_PREDICATE_TEST_TABLE (ID, TIMESERIES_KEY) VALUES (1, CAST(CURRENT_TIME() AS TIMESTAMP))
CREATE TABLE OUTPUT_TEST_TABLE (id BIGINT NOT NULL PRIMARY KEY, col1 VARCHAR, col2 INTEGER, col3 DATE)
CREATE TABLE CUSTOM_ENTITY."z02"(id BIGINT NOT NULL PRIMARY KEY)
UPSERT INTO CUSTOM_ENTITY."z02" (id) VALUES(1)
CREATE TABLE TEST_DECIMAL (ID BIGINT NOT NULL PRIMARY KEY, COL1 DECIMAL(9, 6))
UPSERT INTO TEST_DECIMAL VALUES (1, 123.456789)
CREATE TABLE TEST_SMALL_TINY (ID BIGINT NOT NULL PRIMARY KEY, COL1 SMALLINT, COL2 TINYINT)
UPSERT INTO TEST_SMALL_TINY VALUES (1, 32767, 127)
CREATE TABLE DATE_TEST(ID BIGINT NOT NULL PRIMARY KEY, COL1 DATE)
UPSERT INTO DATE_TEST VALUES(1, CURRENT_DATE())
CREATE TABLE "space" ("key" VARCHAR PRIMARY KEY, "first name" VARCHAR)
UPSERT INTO "space" VALUES ('key1', 'xyz')
CREATE TABLE "small" ("key" VARCHAR PRIMARY KEY, "first name" VARCHAR, "salary" INTEGER )
UPSERT INTO "small" VALUES ('key1', 'foo', 10000)
UPSERT INTO "small" VALUES ('key2', 'bar', 20000)
UPSERT INTO "small" VALUES ('key3', 'xyz', 30000)
