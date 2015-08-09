'use strict';

var TraceCtrl = angular.module('TracingCtrl', ['googlechart', 'ui.bootstrap']);

//listing trace from json
TraceCtrl.controller('TraceListCtrl', function($scope, $http) {
  //$scope.traces = sampleTrace;
  $scope.traces = [];
  $scope.tracesLimit =100;
  
    $scope.loadTrace = function() {
        var httpRequest = $http({
            method: 'GET',
            url: '../trace?action=getall&limit='+$scope.tracesLimit

        }).success(function(data, status) {
            $scope.traces = data;
        });

    };
    $scope.loadTrace();
});

//this will change with after db binding.
TraceCtrl.controller('TraceDepTreeCtrl', function($scope) {
  $scope.rootId = "-6024241463441346911";
  $scope.drawTree = function() {
    $scope.chartObject = dependencyChart;
  }
});

TraceCtrl.controller('TraceCountChartCtrl', function($scope,$http) {
  $scope.page = {
    title: 'Trace Count Chart Graph'
  };
      $scope.loadData = function() {
    console.log('data is loading for getCount');
    $http.get('../trace/?action=getCount').
    success(function(data, status, headers, config) {
      console.log('recived the data');
      console.log(data);
      for(var i = 0; i < data.length; i++) {
        var datax = data[i];
        var datamodel =[{
            "v": datax.description
          }, {
            "v": parseFloat(datax.count)
          }]
        chartObject.data.rows[i] = {"c": datamodel}
        chartObject.options.width = '800';
        chartObject.options.hAxis.title = 'Traces';
      }
    }).
    error(function(data, status, headers, config) {
      console.log('error of loading timeline in start');
    });

    $scope.chartObject = chartObject;
  };
  $scope.chartObject = {};
  $scope.loadData();
  $scope.chartObject.type = "ColumnChart";

});
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
  $scope.chartObject = {};
  $scope.loadData();
  //$scope.chartObject = chartObject
});