'use strict';

var TimeLineCtrl = angular.module('TimeLineCtrl', ['ui.bootstrap']);
TimeLineCtrl.controller('TraceTimeLineCtrl', function($scope, $http) {
  $scope.page = {
    title: 'Timeline for Trace'
  };
  
  
  $scope.clear = function() {
    var nextid = $scope.chartObject.data.rows.length;
    $scope.chartObject.data.rows.splice(nextid - 1, 1);
  }

  $scope.myItemID = "Trace 01";
  $scope.clearId = function() {
    var count = $scope.chartObject.data.rows.length;
    for(var i = 0; i < count; i++) {
      var obj = $scope.chartObject.data.rows[i];
      if(obj === $scope.myItemID) {
        $scope.chartObject.data.rows.splice(i, 1);
      }
    }
  }

  $scope.trace = {};
  $scope.addItemToTimeLine = function(clear) {
    if(clear == true){
      console.log('clear all');
      console.log($scope.chartObject.data.rows);
      $scope.chartObject.data.rows[0].c = [];
    }
    console.log($scope.traceID)
    if($scope.traceID){
      var url = '../trace?action=searchTrace&traceid='+$scope.traceID
      console.log(url);
    $http.get(url).
    success(function(data, status, headers, config) {
      $scope.trace = data[0];
      var nextid = $scope.chartObject.data.rows.length;
      console.log(data[0]);
      //adding to the time line
      for(var i = 0; i < data.length; i++) {
      var currentData = data[i];
      $scope.chartObject.data.rows[nextid+i] = {
        "c": [{
          "v": "Trace " + (nextid+i)
        }, {
          "v": currentData.description
        }, {
          "v": new Date(parseFloat(currentData.start_time))
        }, {
          "v": new Date(parseFloat(currentData.end_time))
        }]
      }
    }
  }).
    error(function(data, status, headers, config) {
      console.log('error in loading time line item');
    });
  }
  else {
    alert('Please, Add trace id');
    }
  };

  $scope.loadTimeLine = function() {
    var limit = 7;
    $http.get('../trace?action=getall&limit='+limit).
    success(function(data, status, headers, config) {
      for(var i = 0; i < data.length; i++) {
        console.log(data[i])
        var datax = data[i];
        var datamodel =[{
            "v": "Trace " + i
          }, {
            "v": datax.description
          }, {
            "v": new Date(parseFloat(datax.start_time))
          }, {
            "v": new Date(parseFloat(datax.end_time))
          }]
        console.log(i)
        timeLine.data.rows[i] = {"c": datamodel
        }
      }
    }).
    error(function(data, status, headers, config) {
      console.log('error of loading timeline in start');
    });
    $scope.chartObject = timeLine;
    $scope.clearId();
  };
  $scope.loadTimeLine();
});