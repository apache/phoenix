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
CREATE VIEW IF NOT EXISTS TEST.SAMPLE_VIEW  (
     DATE_TIME1 DATE NOT NULL,
     INT1 BIGINT NOT NULL,
     SOME_ID CHAR(15) NOT NULL,
     DOUBLE1 DECIMAL(12, 3),
     IS_BOOLEAN BOOLEAN,
     RELATE CHAR(15),
     TEXT1 VARCHAR,
     TEXT_READ_ONLY VARCHAR
     CONSTRAINT PKVIEW PRIMARY KEY
     (
         DATE_TIME1 DESC, INT1, SOME_ID
     )
 )
AS SELECT * FROM TEST.SAMPLE_TABLE_VIEW WHERE FILTER_PREFIX = 'abc';

ALTER VIEW TEST.SAMPLE_VIEW DROP COLUMN DOUBLE1;