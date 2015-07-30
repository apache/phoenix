var barChart = {};
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
    },
    {
"time": "16-18",
"tracecount": 785
    },
    {
"time": "18-20",
"tracecount": 412
    },
    {
"time": "20-22",
"tracecount": 272
    },
    {
"time": "22-24",
"tracecount": 117
    }]
}];