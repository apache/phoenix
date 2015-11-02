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