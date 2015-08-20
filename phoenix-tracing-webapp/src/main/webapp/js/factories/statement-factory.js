'use strict';

/**
 * SQL Convert Statement Factory 
 *
 */

angular.module('TracingAppCtrl').factory('StatementFactory', function(stateConfig) {

  var StatementFactory = function(command,tableName) {
    this.start = stateConfig.start;
    this.command = stateConfig.command[command];
    this.tableName = tableName;
    this.filters = {};
    this.groupBy = {};
    this.keys = {};
    this.end = stateConfig.end;
  };

  StatementFactory.setKeys = function(keys) {
    ChartFactory.keys = keys;
  };

  StatementFactory.setCommands = function(command) {
    ChartFactory.command = command;
  };

  return StatementFactory;
});