/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

CREATE TABLE IF NOT EXISTS SCHEMA_0001.TABLE_0001 (ID1 VARCHAR NOT NULL,
                                                   ID2 VARCHAR NOT NULL,
                                                   COL1 VARCHAR,
                                                   COL2 INTEGER CONSTRAINT pk PRIMARY KEY(ID1, ID2))
    SPLIT ON ('id3', 'id7', 'id9');

UPSERT INTO SCHEMA_0001.TABLE_0001 VALUES ('id1', 'id111', 'col1', 10);
UPSERT INTO SCHEMA_0001.TABLE_0001 VALUES ('id2', 'id112', 'col2', 20);
UPSERT INTO SCHEMA_0001.TABLE_0001 VALUES ('id3', 'id113', 'col3', 30);
UPSERT INTO SCHEMA_0001.TABLE_0001 VALUES ('id4', 'id114', 'col4', 40);
UPSERT INTO SCHEMA_0001.TABLE_0001 VALUES ('id5', 'id115', 'col5', 50);
UPSERT INTO SCHEMA_0001.TABLE_0001 VALUES ('id6', 'id116', 'col6', 60);
UPSERT INTO SCHEMA_0001.TABLE_0001 VALUES ('id7', 'id117', 'col7', 70);
UPSERT INTO SCHEMA_0001.TABLE_0001 VALUES ('id8', 'id118', 'col8', 80);
UPSERT INTO SCHEMA_0001.TABLE_0001 VALUES ('id9', 'id119', 'col9', 90);
UPSERT INTO SCHEMA_0001.TABLE_0001 VALUES ('id10', 'id1110', 'col10', 100);
UPSERT INTO SCHEMA_0001.TABLE_0001 VALUES ('id1', 'id1111', 'col11', 10);
UPSERT INTO SCHEMA_0001.TABLE_0001 VALUES ('id2', 'id1112', 'col12', 20);
UPSERT INTO SCHEMA_0001.TABLE_0001 VALUES ('id3', 'id1113', 'col13', 30);
UPSERT INTO SCHEMA_0001.TABLE_0001 VALUES ('id4', 'id1114', 'col14', 40);
UPSERT INTO SCHEMA_0001.TABLE_0001 VALUES ('id5', 'id1115', 'col15', 50);
UPSERT INTO SCHEMA_0001.TABLE_0001 VALUES ('id6', 'id1116', 'col16', 60);
UPSERT INTO SCHEMA_0001.TABLE_0001 VALUES ('id7', 'id1117', 'col17', 70);
UPSERT INTO SCHEMA_0001.TABLE_0001 VALUES ('id8', 'id1118', 'col18', 80);
UPSERT INTO SCHEMA_0001.TABLE_0001 VALUES ('id9', 'id1119', 'col19', 90);
UPSERT INTO SCHEMA_0001.TABLE_0001 VALUES ('id10', 'id11110', 'col20', 100);
UPSERT INTO SCHEMA_0001.TABLE_0001 VALUES ('id11', 'id11111', 'col21', 111);
UPSERT INTO SCHEMA_0001.TABLE_0001 VALUES ('id12', 'id11112', 'col22', 112);
UPSERT INTO SCHEMA_0001.TABLE_0001 VALUES ('id3', 'id11113', 'col23', 35);
UPSERT INTO SCHEMA_0001.TABLE_0001 VALUES ('id2', 'id11114', 'col24', 25);
