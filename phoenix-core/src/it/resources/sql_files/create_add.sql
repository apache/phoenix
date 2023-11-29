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
 
CREATE TABLE IF NOT EXISTS my_schema.my_table 
                    (id VARCHAR NOT NULL PRIMARY KEY, 
                     name VARCHAR) VERSIONS=1;
UPSERT INTO my_schema.my_table values ('a','a_name');
UPSERT INTO my_schema.my_table values ('b','b_name');

CREATE VIEW IF NOT EXISTS my_table_view (entity_id VARCHAR) 
                    AS SELECT * FROM  my_schema.my_table WHERE id='c';
UPSERT INTO my_table_view (name, entity_id) values ('a_name', 'c_entity');

CREATE TABLE IF NOT EXISTS my_schema.my_table_immutable 
                    (id VARCHAR NOT NULL PRIMARY KEY, name VARCHAR) IMMUTABLE_ROWS=true;
UPSERT INTO my_schema.my_table_immutable values ('a','a_name');
UPSERT INTO my_schema.my_table_immutable values ('b','b_name');

