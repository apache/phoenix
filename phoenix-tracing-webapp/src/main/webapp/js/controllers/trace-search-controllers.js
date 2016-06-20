'use strict';

TraceCtrl.controller('TraceSearchCtrl', function($scope, $http) {
$scope.traceId =0;
$scope.selectedSearchType="trace_id";
$scope.traces = [];
$scope.tracesLimit =100;
$scope.letterLimit =100;

$scope.tabs = [{
            title: 'List',
            url: 'one.tpl.html'
        }, {
            title: 'TimeLine',
            url: 'two.tpl.html'
        }, {
            title: 'Dependency Tree',
            url: 'three.tpl.html'
        }, {
            title: 'Trace Distribution',
            url: 'four.tpl.html'
    }];

  $scope.currentTab = 'one.tpl.html';

  $scope.onClickTab = function (tab) {
    $scope.currentTab = tab.url;
  }

  $scope.searchTrace = function () {
    
    console.log($scope.traceId);
    console.log($scope.selectedSearchType);
    
    if($scope.traceId!=0 && $scope.selectedSearchType=="trace_id")
    $scope.loadTrace();

    if($scope.traceId!=0 && $scope.selectedSearchType=="trace_id")
    $scope.loadTimeLine('../trace?action=searchTrace&traceid='+$scope.traceId);
  };

  $scope.isActiveTab = function(tabUrl) {
    return tabUrl == $scope.currentTab;
  }

  $scope.loadTrace = function() {
        var httpRequest = $http({
            method: 'GET',
            url: '../trace?action=searchTrace&traceid='+$scope.traceId
        }).success(function(data, status) {
            $scope.traces = data;
        });

    };


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
  };



  $scope.loadTimeLine = function(url) {
    $scope.chartObject = timeLine;
    console.log('loadTimeLine. - '+$scope.traceId);
    getTimeLineChart(url);
  };

});
