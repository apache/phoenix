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
    alertType: 'alert-info',
    timelineAlertType: 'alert-info'
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
      $scope.traces = getTraceList(data);
      $scope.chartObject = getTimeLineChart(data);
      $scope.dependencyTreeObject = getTreeData(data);
      $scope.distributionChartObject = getDistData(data, 'hostname');
    });
  };

  //get trace list for list page
  function getTraceList(data) {
    for(var i=0; i<data.length; i++){
      var date = new Date(data[i].start_time);
      data[i]["formated_date"] = date.toString();
    }
    return data;
  }

  //getting TimeLine chart with data
  function getTimeLineChart(data) {
    $scope.timelineStatus = "Retriving data from Phoenix.";
    var currentData = data;
    var minTimeGap = GenerateTimelineService.getMinTimeGap(currentData);
    var mulTime = 1;
    if (minTimeGap < 1000) {
      mulTime = 1000;
    }
    var startDateTime;
    for (var i = 0; i < currentData.length; i++) {
      var datax = currentData[i];
      var toolTip = GenerateTimelineService.getToolTip(datax);
      var dest = GenerateTimelineService.getDescription(datax.description);
      startDateTime = new Date(parseFloat(datax.start_time) * mulTime)
      var datamodel = [{
        "v": "Trace " + i
      }, {
        "v": dest
      }, {
        "v": toolTip
      }, {
        "v": new Date(parseFloat(datax.start_time) * mulTime)
      }, {
        "v": new Date(parseFloat(datax.end_time) * mulTime)
      }]
      timeLine.data.rows[i] = {
        "c": datamodel
      }
    }
    //adding a mock span
    if (minTimeGap == 0) {
      var mockindex = timeLine.data.rows.length
      var t = new Date();
      startDateTime.setMilliseconds(startDateTime.getMilliseconds() + 1);
      var mockdatamodel = [{
        "v": "Trace " + mockindex
      }, {
        "v": "This is a Mock Span"
      }, {
        "v": "This is a Mock Span <br>"
        +"This is added because all other spans durations are zero "
        +"<br>and all have same start time"
      }, {
        "v": timeLine.data.rows[0].c[3].v
      }, {
        "v": startDateTime
      }]
      timeLine.data.rows[mockindex] = {"c": mockdatamodel};
      $scope.page.timelineAlertType = 'alert-warning';
      $scope.timelineStatus = "Duration time of the traces are zero and all have same start time."
      +"Therefore Timeline is not rendered";
      return null;
    }
    timeLine.data.cols = GenerateTimelineService.getTimeLineProperties();
    timeLine.data.options = GenerateTimelineService.getTimeLineOptions();
    $scope.page.timelineAlertType = 'alert-success';
    $scope.timelineStatus = "Data retrieved and Timeline Rendered.";
    return timeLine;
  };


  function setSQLQuery(data) {
    for (var i = 0; i < data.length; i++) {
      var currentParentID = data[i].parent_id;
      for (var j = 0; j < data.length; j++) {
        var currentSpanID = data[j].span_id;
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

  //Controllering distribution chart
  $scope.setChartType = function(type) {
    $scope.distributionChartObject.type = type;
  };

  $scope.setChartAxis = function(type) {
    var chartMeta = GenerateDistributionService.getMateData(type);
    $scope.distributionChartObject.options.vAxis.title = chartMeta.vAxis;
    $scope.distributionChartObject.options.hAxis.title = chartMeta.hAxis;
    $scope.distributionChartObject.data.cols[1].label = chartMeta.label;
    $scope.distributionChartObject.options.width = 1100;
    $scope.distributionChartObject.options['chartArea'] = {left:500, width: 500};
  };

  $scope.setByCount = function(type) {
    $scope.clearDistChart();
    $('#distributionHeader').html('<h1> Trace Distribution By '+type+'</h1>')
    getDistData($scope.currentData, type);
    $scope.setChartAxis(type);
  };


  //cleaning charts
  $scope.cleanTrace = function() {
    $scope.clearTree();
    $scope.cleanTimeline();
    $scope.clearDistChart();
  }

  $scope.cleanTimeline = function() {
    $scope.page.timelineAlertType = 'alert-success';
    $scope.timelineStatus = "Timeline is cleaned.";
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