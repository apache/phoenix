'use strict';

var TimeLineCtrl = angular.module('TimeLineCtrl', ['ui.bootstrap']);
TimeLineCtrl.controller('TraceTimeLineCtrl', function($scope, $http) {
  $scope.page = {
    title: 'Timeline for Trace'
  };
  console.log(timeLine);
  $scope.chartObject = timeLine;
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
  $scope.addItemToTimeLine = function() {
    $http.get('../trace?action=getall&limit=10').
    success(function(data, status, headers, config) {
      $scope.trace = data[0];
      var nextid = $scope.chartObject.data.rows.length;
      console.log(data[0]);
      //adding to the time line
      $scope.chartObject.data.rows[nextid] = {
        "c": [{
          "v": "Trace " + nextid
        }, {
          "v": $scope.trace.description
        }, {
          "v": new Date(parseFloat($scope.trace.start_time))
        }, {
          "v": new Date(parseFloat($scope.trace.end_time))
        }]
      }
    }).
    error(function(data, status, headers, config) {
      console.log('error in loading time line item');
    });
  };

  $scope.loadTimeLine = function() {
    $http.get('../trace?action=getall&limit=5').
    success(function(data, status, headers, config) {

      var nextid = $scope.chartObject.data.rows.length;me
      //adding list of items for time line
      for(var i = 0; i < data.length; i++) {
        var nextid = $scope.chartObject.data.rows.length;
        var datax = data[i];
        console.log(datax);
        $scope.chartObject.data.rows[nextid] = {
          "c": [{
            "v": "Trace " + nextid
          }, {
            "v": datax.description
          }, {
            "v": new Date(parseFloat(datax.start_time))
          }, {
            "v": new Date(parseFloat(datax.end_time))
          }]
        }
      }
    }).
    error(function(data, status, headers, config) {
      console.log('error of loading timeline in start');
    });
  };
  $scope.loadTimeLine();
});