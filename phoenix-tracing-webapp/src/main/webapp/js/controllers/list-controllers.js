'use strict';

var TraceCtrl = angular.module('TracingCtrl', ['googlechart', 'ui.bootstrap']);

//listing trace from json
TraceCtrl.controller('TraceListCtrl', function($scope, $http) {
  //$scope.traces = sampleTrace;
  $scope.traces = [];
  $scope.tracesLimit =100;
  $scope.letterLimit =100;
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