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

    this.getDataModel = function() {
        return cdatamodel;
    };
});