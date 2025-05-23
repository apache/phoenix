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
CREATE TABLE IF NOT EXISTS ABC (NID CHAR(15) NOT NULL, DATA VARCHAR, "a"."_" char(1),
    "b"."_" char(1) CONSTRAINT PK PRIMARY KEY (NID)) VERSIONS=1,MULTI_TENANT=true,REPLICATION_SCOPE=1;
ALTER TABLE ABC SET DISABLE_BACKUP=TRUE;
ALTER TABLE ABC SET BLOOMFILTER='ROW';
ALTER TABLE ABC SET "phoenix.max.lookback.age.seconds"=0;
ALTER TABLE ABC SET UPDATE_CACHE_FREQUENCY=172800000;