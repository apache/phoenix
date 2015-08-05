var timeLine = {};
var chartObject = {
  "type": "PieChart",
  "displayed": true,
  "data": {
    "cols": [
      {
        "id": "node",
        "label": "Node",
        "type": "string",
        "p": {}
      },
      {
        "id": "traceCount",
        "label": "trace Count",
        "type": "number",
        "p": {}
      }
    ],
    "rows": [
      {
        "c": [
          {
            "v": "Node1"
          },
          {
            "v": 0,
          }
        ]
      }
    ]
  },
  "options": {
    "height": 400,
    "width": 700,
    "fill": 20,
    "displayExactValues": true,
    "vAxis": {
      "title": "Trace count",
      "gridlines": {
        "count": 10
      }
    },
    "hAxis": {
      "title": "Node"
    }
  },
  "formatters": {}
}
//getting time line model
timeLine = {
  "type": "Timeline",
  "displayed": true,
  "data": {
    "cols": [
      {
        "id": "TraceID",
        "label": "TraceID",
        "type": "string",
        "p": {}
      },
      {
        "id": "Label",
        "label": "Label",
        "type": "string",
        "p": {}
      },
      {
        "id": "Start",
        "label": "Start",
        "type": "date",
        "p": {}
      },
      {
        "id": "End",
        "label": "End",
        "type": "date",
        "p": {}
      }
    ],
    "rows": [
      {
        "c": [
          {
            "v": "Trace 01"
          },
          {
            "v": "Writing mutation batch for table: MY_TABLE1"
          },
          {
            "v": new Date(1434196101623)
          },
          {
            "v": new Date(1434196101784)
          }
        ]
      }
    ]
  },
  "options": {},
  "formatters": {}
};