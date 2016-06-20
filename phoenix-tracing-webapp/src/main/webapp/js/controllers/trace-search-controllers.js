'use strict';

TraceCtrl.controller('TraceSearchCtrl', function($scope, $http) {

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

  $scope.buttonClick = function () {
    console.log('click');
  };

  $scope.isActiveTab = function(tabUrl) {
    return tabUrl == $scope.currentTab;
  }

});