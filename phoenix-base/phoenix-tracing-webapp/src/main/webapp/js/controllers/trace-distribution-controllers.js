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

TraceCtrl.controller('TraceDistChartCtrl', function($scope, $http) {

  $scope.page = {
    title: 'Trace Distribution'
  };
    $scope.loadData = function() {
    console.log('data is loading for getDistribution');
    $http.get('../trace/?action=getDistribution').
    success(function(data, status, headers, config) {
      console.log('recived the data');
      console.log(data);
      for(var i = 0; i < data.length; i++) {
        var datax = data[i];
        var datamodel =[{
            "v": datax.hostname
          }, {
            "v": parseFloat(datax.count)
          }]
        chartObject.data.rows[i] = {"c": datamodel
        }
      }
    }).
    error(function(data, status, headers, config) {
      console.log('error of loading timeline in start');
    });
    
    $scope.chartObject = chartObject;
  };

  $scope.setChartType = function(type) {
    $scope.chartObject.type = type;
  };

  $scope.chartObject = {};
  $scope.loadData();
  //$scope.chartObject = chartObject
});
