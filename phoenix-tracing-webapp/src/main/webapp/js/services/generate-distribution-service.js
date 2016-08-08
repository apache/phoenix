'use strict';
/**
 * this services was writtern 
 * Generate Distribution Chart model for user interface
 *
 */
angular.module('TracingAppCtrl').service('GenerateDistributionService',
  function() {
    var cmodel = [];

    this.loadData = function(data, byData) {
      this.modelReset();
      for (var i = 0; i < data.length; i++) {
        if (byData == "description") {
          this.countHost(data[i].description);
        } else if (byData == "duration") {
          var localDuration = data[i].end_time - data[i].start_time;
          this.countDuration(data[i].description, localDuration);
        } else {
          this.countDescription(data[i].hostname);
        }
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

    this.modelReset = function() {
      cmodel = [];
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

    this.countDescription = function(localDescription) {
      var isFound = false;
      if (cmodel.length == 0) {
        var obj = {
          'description': localDescription,
          'count': 1
        };
        isFound = true
        cmodel.push(obj);
      } else {
        for (var i = 0; i < cmodel.length; i++) {
          if (cmodel[i].description == localDescription) {
            cmodel[i].count = cmodel[i].count + 1;
            isFound = true;
          }
        }
        if (isFound == false) {
          var obj = {
            'description': localDescription,
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
          'hostname': localDescription,
          'count': localDuration
        };
        isFound = true
        cmodel.push(obj);
      } else {
        for (var i = 0; i < cmodel.length; i++) {
          if (cmodel[i].hostname == localDescription) {
            cmodel[i].count = cmodel[i].count + localDuration;
            isFound = true;
          }
        }
        if (isFound == false) {
          var obj = {
            'hostname': localDescription,
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

  });