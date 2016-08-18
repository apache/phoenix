'use strict';
/**
 * this services was writtern 
 * Generate Distribution Chart model for user interface
 *
 */
angular.module('TracingAppCtrl').service('GenerateDistributionService',
  function() {
    var cmodel = [];
    var chartMetaData = {
      'TraceCount': {
        'vAxis': 'Description',
        'hAxis': 'Trace Count',
        'label': 'Trace Count'
      },
      'Duration': {
        'vAxis': 'Description',
        'hAxis': 'Total Duration (ms)',
        'label': 'Duration (ms)'
      },
      'Hostname': {
        'vAxis': 'Hostname',
        'hAxis': 'Trace Count in Host',
        'label': 'Trace Count'
      }
    };

    this.loadData = function(data, byData) {
      this.modelReset();
      for (var i = 0; i < data.length; i++) {
        if (byData == "TraceCount") {
          this.countDescription(data[i].description);
        } else if (byData == "Duration") {
          var localDuration = data[i].end_time - data[i].start_time;
          this.countDuration(data[i].description, localDuration);
        } else {
          this.countHost(data[i].hostname);
        }
      }
      for (var i = 0; i < cmodel.length; i++) {
        var datax = cmodel[i];
        var datamodel = [{
          "v": datax.label
        }, {
          "v": parseFloat(datax.count)
        }]
        chartObject.data.rows[i] = {
          "c": datamodel
        }
      }
      return chartObject;
    };

    this.modelReset = function() {
      cmodel = [];
    };

    this.countHost = function(localHostname) {
      var isFound = false;
      if (cmodel.length == 0) {
        var obj = {
          'label': localHostname,
          'count': 1
        };
        isFound = true
        cmodel.push(obj);
      } else {
        for (var i = 0; i < cmodel.length; i++) {
          if (cmodel[i].label == localHostname) {
            cmodel[i].count = cmodel[i].count + 1;
            isFound = true;
          }
        }
        if (isFound == false) {
          var obj = {
            'label': localHostname,
            'count': 1
          };
          isFound = true
          cmodel.push(obj);
        }
      }
    };

    this.countDescription = function(localDescription) {
      var isFound = false;
      if (cmodel.length == 0) {
        var obj = {
          'label': localDescription,
          'count': 1
        };
        isFound = true
        cmodel.push(obj);
      } else {
        for (var i = 0; i < cmodel.length; i++) {
          if (cmodel[i].label == localDescription) {
            cmodel[i].count = cmodel[i].count + 1;
            isFound = true;
          }
        }
        if (isFound == false) {
          var obj = {
            'label': localDescription,
            'count': 1
          };
          isFound = true
          cmodel.push(obj);
        }
      }
    };

    this.countDuration = function(localDescription, localDuration) {
      var isFound = false;
      if (cmodel.length == 0) {
        var obj = {
          'label': localDescription,
          'count': localDuration
        };
        isFound = true
        cmodel.push(obj);
      } else {
        for (var i = 0; i < cmodel.length; i++) {
          if (cmodel[i].label == localDescription) {
            cmodel[i].count = cmodel[i].count + localDuration;
            isFound = true;
          }
        }
        if (isFound == false) {
          var obj = {
            'label': localDescription,
            'count': localDuration
          };
          isFound = true
          cmodel.push(obj);
        }
      }
    };

    this.setChartType = function(type) {
      chartObject.type = type;
    };

    this.getMateData = function(dataType) {
      return chartMetaData[dataType];
    }

  });