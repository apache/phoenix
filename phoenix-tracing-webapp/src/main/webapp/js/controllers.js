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
    loadTree();
  }
});

TraceCtrl.controller('TraceCountChartCtrl', function($scope) {
  $scope.page = {
    title: 'Trace Count Chart Graph'
  };
  $scope.chartObject = chartObject;
  $scope.chartObject.type = "ColumnChart";

});
TraceCtrl.controller('TraceDistChartCtrl', function($scope) {

  $scope.page = {
    title: 'Trace Distribution'
  };
  $scope.chartObject = chartObject
});