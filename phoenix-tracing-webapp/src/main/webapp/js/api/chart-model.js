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
            "v": 440,
          }
        ]
      },
      {
        "c": [
          {
            "v": "Node2"
          },
          {
            "v": 350
          }
        ]
      },
      {
        "c": [
          {
            "v": "Node3"
          },
          {
            "v": 546
          }
        ]
      }
    ]
  },
  "options": {
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