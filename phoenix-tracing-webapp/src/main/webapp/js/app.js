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

var TraceApp = angular.module('TracingAppCtrl', [
    'ngRoute',
    'TracingCtrl',
    'UICtrl',
    'TimeLineCtrl',
    'DepTreeCtrl',
    'SearchCtrl'
]);

TraceApp.config(['$routeProvider',
  function($routeProvider) {
    $routeProvider.
      when('/about', {
        templateUrl: 'partials/about.html'
      }).
      when('/search', {
        templateUrl: 'partials/search.html',
        controller: 'SearchTraceCtrl'
      }).
      when('/count-chart', {
        templateUrl: 'partials/chart.html',
        controller: 'TraceCountChartCtrl'
      }).
      when('/trace-distribution', {
        templateUrl: 'partials/chart.html',
        controller: 'TraceDistChartCtrl'
      }).
      when('/trace-timeline', {
        templateUrl: 'partials/google-chart.html',
        controller: 'TraceTimeLineCtrl'
      }).
      when('/help', {
        templateUrl: 'partials/help.html'
      }).
      when('/list', {
        templateUrl: 'partials/list.html',
        controller: 'TraceListCtrl'
      }).
      when('/dependency-tree', {
        templateUrl: 'partials/dependency-tree.html',
        controller: 'TraceDepTreeCtrl'
      }).
      when('/home', {
        templateUrl: 'partials/home.html'
      }).
      when('/dashboard', {
        templateUrl: 'partials/phoenix-trace.html'
      }).
      otherwise({
        redirectTo: '/home'
      });
  }]);
