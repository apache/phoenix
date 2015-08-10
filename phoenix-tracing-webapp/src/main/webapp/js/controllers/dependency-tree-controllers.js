'use strict';

var searchURL = "../trace/?action=searchTrace&traceid=";
var DepTreeCtrl = angular.module('DepTreeCtrl', ['ui.bootstrap']);
DepTreeCtrl.controller('TraceDepTreeCtrl', function($scope, $http, $location) {
  $scope.page = {
    title: 'Dependency Tree for Trace'
  };

  $scope.rootId = "";
  $scope.loadDependencyTree = function(url) {
    var searchObject = $location.search();
    $scope.traceId = searchObject.traceid
    console.log($scope.traceId);
    getTreeData(url + $scope.traceId);
    $scope.chartObject = dependencyChart;
  };

  $scope.drawTree = function() {
    if($scope.traceId != null) {
      getTreeData(searchURL + $scope.traceId);
      $scope.chartObject = dependencyChart;
    } else {
      $scope.reqStatus = "Please Enter TraceID";
    }
  };

  //shortning description
  function getDescription(description) {
    var shortDescription = '';
    var haveBracket = description.indexOf("(");
    if(haveBracket != -1) {
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
      data.trace_id + '\nDuration :  ' + duration;
    return toolTip;
  }

  function getRootID(data) {
    var rootId = null;
    for(var i = 0; i < data.length; i++) {
      console.log('i' + i);
      var foundRoot = false;
      var currentSpanId = data.span_id;
      for(var j = 0; j < data.length; j++) {
        if(currentSpanId == data.parent_id)
          break;
        if(j == data.length - 1) {
          console.log(currentSpanId)
        }
      }
    }
    console.log(data);
    return rootId;
  }

  function getTreeData(url) {
    $scope.reqStatus = "Retriving data from phonix.";
    $http.get(url).
    success(function(data, status, headers, config) {
      getRootID(data);
      for(var i = 0; i < data.length; i++) {
        var currentData = data[i];
        var toolTip = getToolTip(currentData);
        var datamodel = [{
          "v": currentData.span_id
        }, {
          "v": currentData.parent_id
        }, {
          "v": toolTip
        }]
        dependencyChart.data.rows[i] = {
          "c": datamodel
        }
      }
    }).
    error(function(data, status, headers, config) {
      console.log('error of loading dependencyChart');
      $scope.reqStatus = "Error in data retrieving.";
    });
    $scope.reqStatus = "Data retrieved.";
    return dependencyChart;
  };

  $scope.loadDependencyTree(searchURL);
});