'use strict';
/**
 * this services was writtern 
 * Generate Distribution Chart model for user interface
 *
 */
angular.module('TracingAppCtrl').service('GenerateDistributionService', function() {
  var cmodel = [];

  this.loadData = function(data) {
    for (var i = 0; i < data.length; i++) {
      this.countHost(data[i].hostname);
    }

    for (var i = 0; i < cmodel.length; i++) {
      var datax = cmodel[i];
      var datamodel = [{
        "v": datax.hostname
      }, {
        "v": parseFloat(datax.count)
      }]
      chartObject.data.rows[i] = {
        "c": datamodel
      }
    }

    this.setChartType('PieChart');
    return chartObject;
  };

  this.countHost = function(localHostname) {
    var isFound = false;
    if (cmodel.length == 0) {
      var obj = {
        'hostname': localHostname,
        'count': 1
      };
      isFound = true
      cmodel.push(obj);
    } else {
      for (var i = 0; i < cmodel.length; i++) {
        if (cmodel[i].hostname == localHostname) {
          cmodel[i].count = cmodel[i].count + 1;
          isFound = true;
        }
      }
      if (isFound == false) {
        var obj = {
          'hostname': localHostname,
          'count': 1
        };
        isFound = true
        cmodel.push(obj);
      }
    }
  };

  this.setChartType = function(type) {
    chartObject.type = type;
  };

});