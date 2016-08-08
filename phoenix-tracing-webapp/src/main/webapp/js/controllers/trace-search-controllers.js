'use strict';

TraceCtrl.controller('TraceSearchCtrl', function($scope, $http,
  GenerateTimelineService, GenerateDependancyTreeService,
  GenerateDistributionService) {
  $scope.traceId = 0;
  $scope.selectedSearchType = "trace_id";
  $scope.traces = [];
  $scope.tracesLimit = 100;
  $scope.letterLimit = 100;
  $scope.page = {
    alertType: 'alert-info'
  };
  $scope.rootId = "";
  var sqlQuery = null;
  var rootId = null;
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

  $scope.onClickTab = function(tab) {
    $scope.currentTab = tab.url;
  };

  $scope.searchTrace = function() {
    if ($scope.traceId != 0 && $scope.selectedSearchType == "trace_id")
      $scope.loadTrace('../trace?action=searchTrace&traceid=' + $scope.traceId);
    else if ($scope.traceId != null && $scope.selectedSearchType ==
      "host")
      $scope.loadTrace('../trace?action=searchTraceByHost&hostname=' +
        $scope.traceId);
    else if ($scope.traceId != null && $scope.selectedSearchType ==
      "description")
      $scope.loadTrace(
        '../trace?action=searchTraceByDescription&description=' + $scope.traceId
      );
    else if ($scope.traceId != null && $scope.selectedSearchType ==
      "query")
      $scope.loadTrace('../trace?action=searchQuery&query=' + $scope.traceId);
  };

  $scope.isActiveTab = function(tabUrl) {
    return tabUrl == $scope.currentTab;
  };
  $scope.currentData = {};
  $scope.loadTrace = function(searchurl) {
    var httpRequest = $http({
      method: 'GET',
      url: searchurl
    }).success(function(data, status) {
      $scope.currentData = data;
      $scope.traces = data;
      $scope.chartObject = getTimeLineChart(data);
      $scope.dependencyTreeObject = getTreeData(data);
      $scope.distributionChartObject = getDistData(data, 'hostname');
    });
  };

  //getting TimeLine chart with data
  function getTimeLineChart(data) {
    for (var i = 0; i < data.length; i++) {
      var datax = data[i];
      var dest = GenerateTimelineService.getDescription(datax.description);
      var datamodel = [{
        "v": "Trace " + i
      }, {
        "v": dest
      }, {
        "v": new Date(parseFloat(datax.start_time) * 1000)
      }, {
        "v": new Date(parseFloat(datax.end_time) * 1000)
      }, {
        "v": dest
      }]
      timeLine.data.rows[i] = {
        "c": datamodel
      }
    }
    return timeLine;
  };


  function setSQLQuery(data) {
    for (var i = 0; i < data.length; i++) {
      var currentParentID = data[i].parent_id;
      //console.log('p '+currentParentID);
      for (var j = 0; j < data.length; j++) {
        var currentSpanID = data[j].span_id;
        //console.log('s '+currentSpanID);
        if (currentSpanID == currentParentID) {
          break;
        } else if (j == data.length - 1) {
          sqlQuery = data[i].description;
          rootId = currentParentID;
        }
      }
    }
  };

  //get Dependancy tree with data model
  function getTreeData(data) {
    $scope.reqStatus = "Retriving data from Phoenix.";
    //getRootID(data);
    setSQLQuery(data);
    for (var i = 0; i < data.length; i++) {
      var currentData = data[i];
      var currentDataParentId = currentData.parent_id;
      //check for root id
      if (currentDataParentId == rootId) {
        currentDataParentId = '';
      }
      var toolTip = GenerateDependancyTreeService.getToolTip(currentData);
      var datamodel = [{
        "v": currentData.span_id,
        'f': GenerateDependancyTreeService.getDescription(currentData.description)
      }, {
        "v": currentDataParentId
      }, {
        "v": toolTip
      }]
      dependencyChart.data.rows[i] = {
        "c": datamodel
      }
    }
    $scope.page.alertType = 'alert-success';
    $scope.reqStatus = "Data retrieved on SQL Query ";
    return dependencyChart;
  };

  $scope.setChartType = function(type) {
    $scope.distributionChartObject.type = type;
  };

  $scope.setByCount = function(type) {
    $scope.clearDistChart();
    getDistData($scope.currentData, type)
  };


  $scope.cleanTrace = function() {
    $scope.clearTree();
    $scope.cleanTimeline();
    $scope.clearDistChart();
  }

  $scope.cleanTimeline = function() {
    var nextid = $scope.chartObject.data.rows.length;
    $scope.chartObject.data.rows.splice(0, nextid);
  }

  $scope.clearTree = function() {
    if ($scope.dependencyTreeObject != null) {
      for (var i = 0; i < $scope.dependencyTreeObject.data.rows.length; i++) {
        $scope.dependencyTreeObject.data.rows[i] = ClearRow[0];
      }
      $scope.page.alertType = 'alert-info';
      $scope.reqStatus = "Traces are Cleared";
    } else {
      $scope.reqStatus = "There is no Tree to clear";
    }
  };

  $scope.clearDistChart = function() {
    GenerateDistributionService.modelReset();
    if ($scope.distributionChartObject != null) {
      for (var i = 0; i < $scope.distributionChartObject.data.rows.length; i++) {
        $scope.distributionChartObject.data.rows[i] = ClearRow[0];
      }
    }
  };

  function getDistData(data, byData) {
    return GenerateDistributionService.loadData(data, byData);
  };



});