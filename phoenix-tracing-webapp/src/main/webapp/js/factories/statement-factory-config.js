'use strict';
/**
 * @ngdoc function
 * @name TracingAppCtrl.statment configure
 * @description
 * # configures will contains the configure for statement factory. 
 *
 */
angular.module('TracingAppCtrl')
  .constant('stateConfig', {
    'command': {'select': 'FULL SCAN OVER '},
    'start': 'Creating basic query for [',
    'end': ']'
  });