'use strict';
/**
 * this services was writtern 
 * Generate Distribution Chart model for user interface
 *
 */
angular.module('TracingAppCtrl').service('GenerateDistributionService', function() {


this.loadData = function(data) {
   
      for(var i = 0; i < data.length; i++) {
        var datax = data[i];
        var datamodel =[{
            "v": datax.hostname
          }, {
            "v": parseFloat(1)
          }]
        chartObject.data.rows[i] = {"c": datamodel
        }
      }
        
    this.setChartType('PieChart');
    return chartObject;
  };

this.setChartType = function(type) {
    chartObject.type = type;
  };


});