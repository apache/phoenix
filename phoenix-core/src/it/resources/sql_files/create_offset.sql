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

CREATE TABLE IF NOT EXISTS SCHEMA_0002.TABLE_0002 (ID VARCHAR NOT NULL PRIMARY KEY,
                                                   COL1 VARCHAR,
                                                   COL2 INTEGER)
    SPLIT ON ('id5', 'id10', 'id17');

UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('id1','col1', 10);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('id2','col2', 20);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('id3','col3', 30);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('id4','col4', 40);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('id5','col5', 50);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('id6','col6', 30);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('id7','col7', 20);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('id8','col8', 10);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('id9','col9', 40);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('id10','col10', 50);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('id11','col11', 50);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('id12','col12', 40);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('id13','col13', 30);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('id14','col14', 20);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('id15','col15', 10);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('id16','col16', 20);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('id17','col17', 30);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('id18','col18', 30);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('id19','col19', 40);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('id20','col20', 50);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('id21','col21', 60);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('id22','col22', 70);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('id23','col23', 80);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('id24','col24', 90);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('id25','col25', 100);
