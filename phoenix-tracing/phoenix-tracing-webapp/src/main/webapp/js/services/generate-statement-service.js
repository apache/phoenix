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

'use strict';
/**
 * this services was writtern following different pattern.
 * Generate Statement Service mainly converting SQL Statement to Tracing Decription Label
 * To-Do Switching controllers to this pattern of coding
 *
 */
angular.module('TracingAppCtrl').service('GenerateStatementService', function(
  StatementFactory) {
  /*using statement facotry - It is in progress*/
  var SQLQuery = null;
  var tracingStatement = null;
  //following Grammar @ http://phoenix.apache.org/language/index.html
  //To-Do this model will improve as developing is going on
  var SQLQueryObject = {
    commands: {},
    keys: [],
    schema: null,
    tabel: null,
    filters: {},
    groupBy: {},
    orderBy: {},
    limits: {}
  }
  this.setSQLQuery = function(sqlQuery) {
    SQLQuery = sqlQuery;
  };
  this.getSQLQuery = function() {
    return SQLQuery;
  };
  this.getTracingStatement = function() {
    //will using partitioningSQLQuery to convert SQL to TracingStatement
    partitioningSQLQuery();
    var statementFactory = new StatementFactory(SQLQueryObject.commands,
      SQLQueryObject.tabel);
    tracingStatement = statementFactory.start + statementFactory.command +
      statementFactory.tableName + statementFactory.end;
    return tracingStatement;
  };
  //sql statements partitioning
  function partitioningSQLQuery() {
    //Building SQLQueryObject
    SQLQueryObject.commands = SQLQuery.split(" ")[0];
    SQLQueryObject.tabel = SQLQuery.split("from ")[1].split(" ")[0];
  };
});
