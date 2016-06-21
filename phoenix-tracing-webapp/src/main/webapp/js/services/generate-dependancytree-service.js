'use strict';
/**
 * this services was writtern 
 * Generate Dependancy tree model for user interface
 *
 */
angular.module('TracingAppCtrl').service('GenerateDependancyTreeService', function() {

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

  //shortning description
  this.getDescription = function(description) {
    var shortDescription = '';
    var haveBracket = description.indexOf("(");
    if (haveBracket != -1) {
      shortDescription = description.substring(0, description.indexOf("("))
    } else {
      shortDescription = description;
    }
    return shortDescription;
  };

  this.getToolTip = function(data) {
    var toolTip = '';
    var dst = this.getDescription(data.description);
    var start_time = new Date(parseFloat(data.start_time));
    var end_time = new Date(parseFloat(data.end_time));
    var duration = (data.end_time - data.start_time) + ' ns';
    var hostname = data.hostname;
    toolTip = 'Hostname :  ' + hostname + '\nDescription :  ' + dst +
      '\nStart At :  ' + start_time + '\nEnd At :  ' + end_time +
      '\nTrace Id :  ' +
      data.trace_id + '\nParent Id :  ' + data.parent_id + '\nSpan Id :  ' +
      data.span_id + '\nDuration :  ' + duration;
    return toolTip;
  };



});