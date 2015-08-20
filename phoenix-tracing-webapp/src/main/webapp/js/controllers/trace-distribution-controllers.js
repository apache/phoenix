'use strict';

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

  $scope.setChartType = function(type) {
    $scope.chartObject.type = type;
  };

  $scope.chartObject = {};
  $scope.loadData();
  //$scope.chartObject = chartObject
});