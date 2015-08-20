'use strict';

angular.module('TimeLineCtrl')
  .constant('chartModels', {
      'timeLineModel': {
        "type": "Timeline",
        "displayed": true,
        "data": {
          "cols": [{
            "id": "TraceID",
            "label": "TraceID",
            "type": "string",
            "p": {}
          }, {
            "id": "Label",
            "label": "Label",
            "type": "string",
            "p": {}
          }, {
            "id": "Start",
            "label": "Start",
            "type": "date",
            "p": {}
          }, {
            "id": "End",
            "label": "End",
            "type": "date",
            "p": {}
          }],
          "rows": [{
            "c": [{
              "v": "Trace 01"
            }, {
              "v": "Writing mutation batch for table: MY_TABLE1"
            }, {
              "v": new Date(1434196101623)
            }, {
              "v": new Date(1434196101784)
            }]
          }]
        },
        "options": {},
        "formatters": {}
      }
    });