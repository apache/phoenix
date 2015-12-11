'use strict';

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

  $scope.setChartType = function(type) {
    $scope.chartObject.type = type;
  };

  $scope.chartObject = {};
  $scope.loadData();
  $scope.chartObject.type = "ColumnChart";

});