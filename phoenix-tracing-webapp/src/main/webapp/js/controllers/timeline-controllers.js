'use strict';

var TimeLineCtrl = angular.module('TimeLineCtrl', ['ui.bootstrap']);
TimeLineCtrl.controller('TraceTimeLineCtrl', function($scope, $http) {
  $scope.page = {
    title: 'Timeline for Trace'
  };


  $scope.clear = function() {
    var nextid = $scope.chartObject.data.rows.length;
    $scope.chartObject.data.rows.splice(1, nextid - 2);
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
    if(clear == true) {
      $scope.chartObject.data.rows[0].c = [];
      $scope.loadTimeLine('../trace?action=searchTrace&traceid=' + $scope.traceID);
    } else {
      console.log($scope.traceID)
      if($scope.traceID) {
        var url = '../trace?action=searchTrace&traceid=' + $scope.traceID
        $http.get(url).
        success(function(data, status, headers, config) {
          $scope.trace = data[0];
          var nextid = $scope.chartObject.data.rows.length;
          console.log(data[0]);
          //adding to the time line
          for(var i = 0; i < data.length; i++) {
            var currentData = data[i];
            var dest = getDescription(currentData.description);
            $scope.chartObject.data.rows[nextid + i] = {
              "c": [{
                "v": "Trace " + (nextid + i)
              }, {
                "v": dest
              }, {
                "v": new Date(parseFloat(currentData.start_time))
              }, {
                "v": new Date(parseFloat(currentData.end_time))
              }]
            }
          } //end of for loop
        }).
        error(function(data, status, headers, config) {
          console.log('error in loading time line item');
        });
      } else {
        alert('Please, Add trace id');
      }
    }
  };

  $scope.loadTimeLine = function(url) {

    $http.get(url).
    success(function(data, status, headers, config) {
      for(var i = 0; i < data.length; i++) {
        console.log(data[i])
        var datax = data[i];
        var dest = getDescription(datax.description);
        var datamodel = [{
          "v": "Trace " + i
        }, {
          "v": dest
        }, {
          "v": new Date(parseFloat(datax.start_time))
        }, {
          "v": new Date(parseFloat(datax.end_time))
        }]
        console.log(i)
        timeLine.data.rows[i] = {
          "c": datamodel
        }
      }
    }).
    error(function(data, status, headers, config) {
      console.log('error of loading timeline in start');
    });
    $scope.chartObject = timeLine;
    $scope.clearId();
  };

  //shortning description
  function getDescription(description) {
    var dst = '';
    var haveBracket = description.indexOf("(");
    if(haveBracket != -1) {
      dst = description.substring(0, description.indexOf("("))
    } else {
      dst = description;
    }
    console.log(dst);
    return dst;
  }
  $scope.loadTimeLine('../trace?action=getall&limit=7');
});