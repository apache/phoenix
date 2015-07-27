var pieChart = {};
pieChart.options = {
    chart: {
        type: 'pieChart',
        height: 300,
        x: function(d) {
            return d.key;
        },
        y: function(d) {
            return d.y;
        },
        showLabels: true,
        transitionDuration: 500,
        labelThreshold: 0.01,
        legend: {
            margin: {
                top: 5,
                right: 35,
                bottom: 5,
                left: 0
            }
        }
    }
};
pieChart.data = [{
    key: "Node1",
    y: 24
}, {
    key: "Node2",
    y: 5
}, {
    key: "Node3",
    y: 15
}, {
    key: "Node4",
    y: 7
}];