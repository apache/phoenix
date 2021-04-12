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

CREATE TABLE IF NOT EXISTS S.T (A INTEGER PRIMARY KEY, B INTEGER, C VARCHAR, D INTEGER);
CREATE VIEW IF NOT EXISTS S.V (VA INTEGER, VB INTEGER) AS SELECT * FROM S.T WHERE B=200;
UPSERT INTO S.V (A, B, C, D, VA, VB) VALUES (2, 200, 'def', -20, 91, 101);
ALTER VIEW S.V DROP COLUMN C;
