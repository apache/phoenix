/*
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
*/
CREATE TABLE IF NOT EXISTS PHERF.TEST_TABLE (
    TENANT_ID CHAR(15) NOT NULL,
    PARENT_ID CHAR(15) NOT NULL,
    CREATED_DATE DATE NOT NULL,
    NOW_DATE DATE,
    PRESENT_DATE DATE,
    OTHER_ID CHAR(15),
    FIELD VARCHAR,
    OLDVAL_STRING VARCHAR,
    NEWVAL_STRING VARCHAR,
    SOME_INT INTEGER
    CONSTRAINT PK PRIMARY KEY
    (
        TENANT_ID,
        PARENT_ID,
        CREATED_DATE DESC
    )
) VERSIONS=1,MULTI_TENANT=true
