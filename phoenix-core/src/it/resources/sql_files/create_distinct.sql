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

CREATE TABLE IF NOT EXISTS SCHEMA_0002.TABLE_0002 (ID1 VARCHAR NOT NULL,
                                                   ID2 VARCHAR NOT NULL,
                                                   ID3 VARCHAR NOT NULL,
                                                   COL1 VARCHAR,
                                                   COL2 INTEGER CONSTRAINT PK PRIMARY KEY (ID1, ID2, ID3));

UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('a','1','x','data1', 10);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('a','1','y','data2', 20);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('a','2','x','data3', 30);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('a','2','y','data4', 40);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('b','1','x','data5', 50);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('b','1','y','data6', 60);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('b','2','x','data7', 70);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('b','2','y','data8', 80);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('c','1','x','data9', 90);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('c','1','y','data10', 100);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('c','2','x','data11', 110);
UPSERT INTO SCHEMA_0002.TABLE_0002 VALUES ('c','2','y','data12', 120);