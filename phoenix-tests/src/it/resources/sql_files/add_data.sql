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
 
UPSERT INTO my_schema.my_table values ('x','a_name');
UPSERT INTO my_table_view (id, entity_id) VALUES ('y', 'y_entity');
UPSERT INTO my_schema.my_table_immutable values ('x','x_name');

CREATE VIEW IF NOT EXISTS my_table_second_view (entity_id VARCHAR) 
                    AS SELECT * FROM  my_schema.my_table WHERE name='b_name';
UPSERT INTO my_table_second_view (id, entity_id) values ('z', 'z_entity');

UPSERT INTO my_table_view (id, entity_id) values ('d', 'd_entity');