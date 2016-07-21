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

var DepTreeCtrl = angular.module('DepTreeCtrl', ['ui.bootstrap']);
DepTreeCtrl.controller('TraceDepTreeCtrl', function($scope, $http, $location) {
  var searchURL = "../trace/?action=searchTrace&traceid=";
  var sqlQuery = null;
  var rootId = null;
  $scope.page = {
    title: 'Dependency Tree for Trace'
  };
  $scope.page = {
    alertType: 'alert-info'
  };
  
  $scope.rootId = "";
  $scope.loadDependencyTree = function(url) {
    $scope.clearTree();
    $scope.page.alertType = 'alert-info';
    $scope.reqStatus = "Loading Phoenix Tracing data";
    var searchObject = $location.search();
    $scope.traceId = searchObject.traceid
    if($scope.traceId != null){
      getTreeData(url + $scope.traceId);
      $scope.chartObject = null;
      $scope.chartObject = dependencyChart;
    }else{
      $scope.page.alertType = 'alert-warning';
      $scope.reqStatus = "Please Enter the TraceID.";
    }
  };

  $scope.drawTree = function() {
    $scope.clearTree();
    if ($scope.traceId != null) {
      getTreeData(searchURL + $scope.traceId);
      $scope.chartObject = dependencyChart;
    } else {
      $scope.reqStatus = "Please Enter TraceID";
    }
  };

  $scope.clearTree = function() {
    if ($scope.chartObject != null) {
      for (var i = 0; i < $scope.chartObject.data.rows.length; i++) {
        $scope.chartObject.data.rows[i] = ClearRow[0];
      }
      $scope.page.alertType = 'alert-info';
      $scope.reqStatus = "Tree is Cleared";
    } else {
      $scope.reqStatus = "There is no Tree to clear";
    }
  };

  //shortning description
  function getDescription(description) {
    var shortDescription = '';
    var haveBracket = description.indexOf("(");
    if (haveBracket != -1) {
      shortDescription = description.substring(0, description.indexOf("("))
    } else {
      shortDescription = description;
    }
    return shortDescription;
  }

  function getToolTip(data) {
    var toolTip = '';
    var dst = getDescription(data.description);
    var start_time = new Date(parseFloat(data.start_time));
    var end_time = new Date(parseFloat(data.end_time));
    var duration = (data.end_time - data.start_time) + ' ns';
    var hostname = data.hostname;
    toolTip = 'Hostname :  ' + hostname + '\nDescription :  ' + dst +
      '\nStart At :  ' + start_time + '\nEnd At :  ' + end_time +
      '\nTrace Id :  ' +
      data.trace_id + '\nParent Id :  ' + data.parent_id + '\nSpan Id :  ' +
      data.span_id + '\nDuration :  ' + duration;
    return toolTip;
  }

  function setSQLQuery(data) {
    for (var i = 0; i < data.length; i++) {
      var currentParentID = data[i].parent_id;
      //console.log('p '+currentParentID);
      for (var j = 0; j < data.length; j++) {
        var currentSpanID = data[j].span_id;
        //console.log('s '+currentSpanID);
        if (currentSpanID == currentParentID) {
          break;
        } else if (j == data.length - 1) {
          sqlQuery = data[i].description;
          rootId = currentParentID;
        }
      }
    }
  }

  //get Dependancy tree with data model
  function getTreeData(url) {
    $scope.reqStatus = "Retriving data from Phoenix.";
    $http.get(url).
    success(function(data, status, headers, config) {
      //getRootID(data);
      setSQLQuery(data);
      for (var i = 0; i < data.length; i++) {
        var currentData = data[i];
        var currentDataParentId = currentData.parent_id;
        //check for root id
        if (currentDataParentId == rootId) {
          currentDataParentId = '';
        }
        var toolTip = getToolTip(currentData);
        var datamodel = [{
          "v": currentData.span_id,
          'f': getDescription(currentData.description)
        }, {
          "v": currentDataParentId
        }, {
          "v": toolTip
        }]
        dependencyChart.data.rows[i] = {
          "c": datamodel
        }
      }
      $scope.page.alertType = 'alert-success';
      $scope.reqStatus = "Data retrieved on SQL Query - " +
        sqlQuery;
    }).
    error(function(data, status, headers, config) {
      console.log('error of loading dependencyChart');
      $scope.page.alertType = 'alert-warning';
      if ($scope.traceId == null) {
        $scope.reqStatus = "Please Enter the TraceID.";
      } else {
        $scope.reqStatus = "Error in data retrieving.";
      }
    });
    //$scope.reqStatus = "Data retrieved on "+sqlQuery;
    return dependencyChart;
  };

  $scope.loadDependencyTree(searchURL);
});
