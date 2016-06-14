'use strict';
/**
 * this services was writtern 
 * Generate timeline model for user interface
 * To-Do timeline chart model genrate
 *
 */
angular.module('TracingAppCtrl').service('GenerateTimelineService', function() {

  //trace item to show current time
  var cdatamodel = [{
    "v": "Trace 01"
  }, {
    "v": "Current Time"
  }, {
    "v": new Date()
  }, {
    "v": new Date()
  }, {
    "v": "Current time"
  }];


  var timeLineColProperty = [{
    type: 'string',
    id: 'Trace ID'
  }, {
    type: 'string',
    id: 'Bar label'
  }, {
    type: "string",
    role: "tooltip",
    p: {
      'html': true
    }
  }, {
    type: 'date',
    id: 'Start'
  }, {
    type: 'date',
    id: 'End'
  }];

  var timeLineOptions = {
    timeline: {
      tooltip: {
        isHtml: true
      }
    }
  };

  var trace_start_time;
  var trace_end_time;
/*      $scope.chartObject.options.vAxis.title = "chartMeta.vAxis";
    $scope.chartObject.options.hAxis.title = "chartMeta.hAxis";
    $scope.chartObject.data.cols[1].label = "chartMeta.label";
    $scope.chartObject.options.width = 500;
    */
  this.getDescription = function(description) {
    var dst = '';
    var haveBracket = description.indexOf("(");
    if (haveBracket != -1) {
      dst = description.substring(0, description.indexOf("("))
    } else {
      dst = description;
    }
    return dst;
  };

  this.getTimeLineProperties = function() {
    return timeLineColProperty;
  };

  this.getTimeLineStartTime = function() {
    return trace_start_time;
  };

  this.getTimeLineEndTime = function() {
    return trace_end_time;
  };

  this.getTimeLineOptions = function() {
    return timeLineOptions;
  };

  this.getMinTimeGap = function(data) {
    var low = data[0].start_time,
      high = data[0].end_time;
    for (var i = 1; i < data.length; i++) {
      if (data[i].start_time < low) {
        low = data[i].start_time;
      }
      if (data[i].end_time > high) {
        high = data[i].end_time;
      }
    }
    trace_start_time = low;
    trace_end_time = high;
    return (high - low);
  };

  this.getToolTip = function(data) {
    var toolTip = '';
    var dst = this.getDescription(data.description);
    var start_time = new Date(parseFloat(data.start_time));
    var end_time = new Date(parseFloat(data.end_time));
    var duration = (data.end_time - data.start_time) + ' ms';
    var hostname = data.hostname;
    toolTip = '<p>Hostname :  ' + hostname + '<br>Description :  ' + dst +
      '<br>Start At :  ' + start_time + '<br>End At :  ' + end_time +
      '<br>Trace Id :  ' +
      data.trace_id + '<br>Parent Id :  ' + data.parent_id +
      '<br>Span Id :  ' +
      data.span_id + '<br>Duration :  ' + duration + '</p>';
    return toolTip;
  };

  this.getDataModel = function() {
    return cdatamodel;
  };
});