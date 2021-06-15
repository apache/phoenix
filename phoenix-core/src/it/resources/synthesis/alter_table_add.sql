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
CREATE TABLE IF NOT EXISTS TEST.SAMPLE_TABLE (
   ORG_ID CHAR(15) NOT NULL,
   SOME_ANOTHER_ID BIGINT NOT NULL,
   SECOND_ID BIGINT NOT NULL,
   TYPE VARCHAR,
   STATUS VARCHAR,
   START_TIMESTAMP BIGINT,
   END_TIMESTAMP BIGINT,
   PARAMS VARCHAR,   RESULT VARCHAR
   CONSTRAINT PK PRIMARY KEY (ORG_ID, SOME_ANOTHER_ID, SECOND_ID)
) VERSIONS=1,MULTI_TENANT=FALSE,REPLICATION_SCOPE=1,TTL=31536000;

ALTER TABLE TEST.SAMPLE_TABLE ADD IF NOT EXISTS RELATED_COMMAND BIGINT DEFAULT 100;