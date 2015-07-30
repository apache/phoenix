'use strict';

var TraceCtrl = angular.module('TracingCtrl', ['nvd3', 'googlechart', 'ui.bootstrap']);

//listing trace from json
TraceCtrl.controller('TraceListCtrl', function($scope) {
  $scope.traces = sampleTrace;
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
  $scope.options = barChart.options;
  $scope.data = barChart.data;

});
TraceCtrl.controller('TraceDistChartCtrl', function($scope) {
  $scope.options = barChart.options;

  $scope.setChartType = function (chartType){
    console.log($scope.options.chart.type);
    $scope.options.chart.type = chartType;
  }

  $scope.page = {
    title: 'Trace Distribution'
  };
  $scope.data = barChart.data;
  $scope.distributeTypes = [
      {name:'By Time'},
      {name:'By Nodes'}
    ];
  $scope.myDistributeType = $scope.distributeTypes[0];
});