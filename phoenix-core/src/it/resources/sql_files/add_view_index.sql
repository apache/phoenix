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

CREATE TABLE MY_SCHEMA.VIEW_INDEX_BASE_TABLE (TENANT_ID CHAR(15) NOT NULL, ID CHAR(3) NOT NULL, NUM BIGINT CONSTRAINT PK PRIMARY KEY (TENANT_ID, ID)) MULTI_TENANT = true;
CREATE VIEW MY_SCHEMA.GLOBAL_VIEW (A BIGINT PRIMARY KEY, B BIGINT) AS SELECT * FROM MY_SCHEMA.VIEW_INDEX_BASE_TABLE WHERE ID='ABC';
CREATE INDEX MY_SCHEMA_VIEW_INDEX ON MY_SCHEMA.GLOBAL_VIEW (B DESC) INCLUDE (NUM);