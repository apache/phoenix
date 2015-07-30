var barChart = {};
var pieChart = {};
var timeLine = {};

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
      },
	   {
        "c": [
          {
            "v": "Trace 02"
          },
          {
            "v": "Parallel scanner for table: MY_TABLE1"
          },
          {
            "v": new Date(1434196184377)
          },
          {
            "v": new Date(1434196184388)
          }
        ]
      },
	  {
        "c": [
          {
            "v": "Trace 03"
          },
          {
            "v": "Creating basic query for [CLIENT 1-CHUNK PARALLEL 1-WAY POINT LOOKUP ON 1 KEY OVER MY_TABLE1]"
          },
          {
            "v": new Date(1434196132606)
          },
          {
            "v": new Date(1434196132648)
          }
        ]
      },
      {
        "c": [
          {
            "v": "Trace 04"
          },
          {
            "v": "Parallel scanner for table: MY_TABLE1"
          },
          {
            "v":  new Date(1434194860485)
          },
          {
            "v":  new Date(1434194860491)
          }          
        ]
      }      
    ]
  },
  "options": {},
  "formatters": {}
};
pieChart.options = {
  chart: {
    type: "cumulativeLineChart",
    height: 400,
    width: 900,
    showLabels: true,
    transitionDuration: 500,
    labelThreshold: 0.01,
    x: function(d) {
      return d.key;
    },
    y: function(d) {
      return d.y;
    },
  }
};

pieChart.data = [{
  key: "Node One",
  y: 404
}, {
  key: "Node Two",
  y: 120
}, {
  key: "Node Three",
  y: 514
}, {
  key: "Node Four",
  y: 228
}, {
  key: "Node Five",
  y: 356
}, {
  key: "Node Six",
  y: 37
}];
barChart.options = {
  chart: {
    type: "discreteBarChart",
    height: 400,
    width: 900,
    margin: {
      top: 20,
      right: 50,
      bottom: 20,
      left: 150
    },
    x: function(d) {
      return d.time;
    },
    y: function(d) {
      return d.tracecount;
    },
    showValues: true,
    transitionDuration: 500,
    xAxis: {
      axisLabel: "Time (hours)"
    },
    yAxis: {
      axisLabel: "Trace Count",
      axisLabelDistance: 10
    }
  }
};
barChart.data = [{

  values: [{
    "time": "8-10",
    "tracecount": 550
  }, {
    "time": "10-12",
    "tracecount": 880
  }, {
    "time": "12-14",
    "tracecount": 623
  }, {
    "time": "14-16",
    "tracecount": 940
  }, {
    "time": "16-18",
    "tracecount": 785
  }, {
    "time": "18-20",
    "tracecount": 412
  }, {
    "time": "20-22",
    "tracecount": 272
  }, {
    "time": "22-24",
    "tracecount": 117
  }]
}];