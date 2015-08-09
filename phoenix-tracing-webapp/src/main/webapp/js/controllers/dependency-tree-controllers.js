'use strict';

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
    getTreeData(url+$scope.traceId);
    $scope.chartObject = dependencyChart;
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

  function getRootID(data) {
    var rootId = null;
    for(var i = 0; i < data.length; i++) {
      console.log('i' + i);
      var foundRoot = false;
      var currentSpanId = data.span_id;
      for(var j = 0; j < data.length; j++) {
        if(currentSpanId == data.parent_id)
          break;
        if(j == data.length-1){
        console.log(currentSpanId)
        }
      }
    }
    console.log(data);
    return rootId;
  }

  function getTreeData(url) {
    $http.get(url).
    success(function(data, status, headers, config) {
      getRootID(data);
      for(var i = 0; i < data.length; i++) {
        console.log(data[i])
        var currentData = data[i];
        var dest = getDescription(currentData.description);
        var datamodel = [{
          "v": currentData.span_id
        }, {
          "v": currentData.parent_id
        }, {
          "v": dest
        }]
        console.log(i)
        dependencyChart.data.rows[i] = {
          "c": datamodel
        }
      }
    }).
    error(function(data, status, headers, config) {
      console.log('error of loading timeline in start');
    });
    console.log(dependencyChart);
    return dependencyChart;
  };

  $scope.loadDependencyTree('../trace/?action=searchTrace&traceid=');
});