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

var TimeLineCtrl = angular.module('TimeLineCtrl', ['ui.bootstrap']);
TimeLineCtrl.controller('TraceTimeLineCtrl', function($scope, $http, $location) {
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
    for (var i = 0; i < count; i++) {
      var obj = $scope.chartObject.data.rows[i];
      if (obj === $scope.myItemID) {
        $scope.chartObject.data.rows.splice(i, 1);
      }
    }
  }

  $scope.trace = {};

  //Appending items for time line
  $scope.addItemToTimeLine = function(clear) {
    if (clear == true) {
      $scope.chartObject.data.rows[0].c = [];
      $scope.loadTimeLine('../trace?action=searchTrace&traceid=' + $scope
        .traceID);
    } else {
      console.log($scope.traceID)
      if ($scope.traceID) {
        var url = '../trace?action=searchTrace&traceid=' + $scope.traceID
        $http.get(url).
        success(function(data, status, headers, config) {
          $scope.trace = data[0];
          var nextid = $scope.chartObject.data.rows.length;
          console.log(data[0]);
          //adding to the time line
          for (var i = 0; i < data.length; i++) {
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
              }, {
                "v": dest
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

  //loading timeline items for exisiting timeline chart
  $scope.loadTimeLine = function(url) {
    var searchURL = "../trace/?action=searchTrace&traceid=";
    $scope.chartObject = timeLine;
    var searchObject = $location.search();
    $scope.traceID = searchObject.traceid
    console.log($scope.traceID);
    var newsurl = searchURL + $scope.traceID;
    if ($scope.traceID == null) {
      getTimeLineChart(url);
    } else {
      //TODO draw chart in start of the page.
      getTimeLineChart(newsurl);
      //getTimeLineChart(url);
    }

    $scope.clearId();
  };

  //shortning description
  function getDescription(description) {
    var dst = '';
    var haveBracket = description.indexOf("(");
    if (haveBracket != -1) {
      dst = description.substring(0, description.indexOf("("))
    } else {
      dst = description;
    }
    console.log(dst);
    return dst;
  }

  //trace item to show current time
  var cdatamodel = [{
    "v": "Trace 01"
  }, {
    "v": "Current Time"
  }, {
    "v": new Date()
  }, {
    "v": new Date()
  }, {
    "v": "Current time"
  }];

  //getting TimeLine chart with data
  function getTimeLineChart(url) {
    $http.get(url).
    success(function(data, status, headers, config) {
      for (var i = 0; i < data.length; i++) {
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
        }, {
          "v": dest
        }]
        console.log(i)
        timeLine.data.rows[i] = {
          "c": datamodel
        }
      }
      timeLine.data.rows[data.length] = {
        "c": cdatamodel
      }
      console.log(timeLine);
      $scope.chartObject = timeLine;
    }).
    error(function(data, status, headers, config) {
      console.log('error of loading timeline in start');
    });
    console.log(timeLine);
    return timeLine;
  }

  $scope.loadTimeLine('../trace?action=getall&limit=7');
});
