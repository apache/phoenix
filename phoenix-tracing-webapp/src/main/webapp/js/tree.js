loadTree = function() {      
  //depency tree
  var json = 
    {
    "name": "-6024241463441346911",
    "children": [
        {
            "name": "477902",
            "children": [
                {
                    "name": "257244986259751239",
                    "children": [
                        {"name": "751217746251631535", 
                         "children": [
                            {"name": "-7194992208996809874"},
                            {"name": "-1209185094638930051"}
                        ]},
                        {"name": "-562612877153057404"},
                        {"name": "-2960497559308033431"}
                    ]
                },
                {
                    "name": "-7926088039726831839",
                    "children": [
                        {"name": "-3932683727973048095"},
                        {"name": "-4101661092039940992"}
                    ]
                }
            ]
        },
        {
            "name": "-159715218972561410",
            "children": [
                {
                    "name": "5692103407460593811",
                    "children": [
                        {"name": "2497415393999835586"},
                        {"name": "-7194992208996809874"},
                        {"name": "-7887359433571201709"}
                    ]
                },
                {
                    "name": "5310738637332790974",
                    "children": [
                        {"name": "-2960497559308033431"},
                        {"name": "7887359433571201709"},
                        {"name": "-3932683727973048095"}
                    ]
                }
            ]
        }
    ]
};

var width = 900;
var height = 650;
var maxLabel = 150;
var duration = 500;
var radius = 5;
    
var i = 0;
var root;

var tree = d3.layout.tree()
    .size([height, width]);

var diagonal = d3.svg.diagonal()
    .projection(function(d) { return [d.y, d.x]; });

var svg = d3.select("#deptree").append("svg")
    .attr("width", width)
    .attr("height", height)
        .append("g")
        .attr("transform", "translate(" + maxLabel + ",0)");
        

root = json;
root.x0 = height / 2;
root.y0 = 0;

root.children.forEach(collapse);

function update(source) 
{
    // Compute the new tree layout.
    var nodes = tree.nodes(root).reverse();
    var links = tree.links(nodes);

    // Normalize for fixed-depth.
    nodes.forEach(function(d) { d.y = d.depth * maxLabel; });

    // Update the nodes…
    var node = svg.selectAll("g.node")
        .data(nodes, function(d){ 
            return d.id || (d.id = ++i); 
        });

    // Enter any new nodes at the parent's previous position.
    var nodeEnter = node.enter()
        .append("g")
        .attr("class", "node")
        .attr("transform", function(d){ return "translate(" + source.y0 + "," + source.x0 + ")"; })
        .on("click", click);

    nodeEnter.append("circle")
        .attr("r", 0)
        .style("fill", function(d){ 
            return d._children ? "lightsteelblue" : "white"; 
        });

    nodeEnter.append("text")
        .attr("x", function(d){ 
            var spacing = computeRadius(d) + 5;
            return d.children || d._children ? -spacing : spacing; 
        })
        .attr("dy", "3")
        .attr("text-anchor", function(d){ return d.children || d._children ? "end" : "start"; })
        .text(function(d){ return d.name; })
        .style("fill-opacity", 0);

    // Transition nodes to their new position.
    var nodeUpdate = node.transition()
        .duration(duration)
        .attr("transform", function(d) { return "translate(" + d.y + "," + d.x + ")"; });

    nodeUpdate.select("circle")
        .attr("r", function(d){ return computeRadius(d); })
        .style("fill", function(d) { return d._children ? "lightsteelblue" : "#fff"; });

    nodeUpdate.select("text").style("fill-opacity", 1);

    // Transition exiting nodes to the parent's new position.
    var nodeExit = node.exit().transition()
        .duration(duration)
        .attr("transform", function(d) { return "translate(" + source.y + "," + source.x + ")"; })
        .remove();

    nodeExit.select("circle").attr("r", 0);
    nodeExit.select("text").style("fill-opacity", 0);

    // Update the links…
    var link = svg.selectAll("path.link")
        .data(links, function(d){ return d.target.id; });

    // Enter any new links at the parent's previous position.
    link.enter().insert("path", "g")
        .attr("class", "link")
        .attr("d", function(d){
            var o = {x: source.x0, y: source.y0};
            return diagonal({source: o, target: o});
        });

    // Transition links to their new position.
    link.transition()
        .duration(duration)
        .attr("d", diagonal);

    // Transition exiting nodes to the parent's new position.
    link.exit().transition()
        .duration(duration)
        .attr("d", function(d){
            var o = {x: source.x, y: source.y};
            return diagonal({source: o, target: o});
        })
        .remove();

    // Stash the old positions for transition.
    nodes.forEach(function(d){
        d.x0 = d.x;
        d.y0 = d.y;
    });
}

function computeRadius(d)
{
    if(d.children || d._children) return radius + (radius * nbEndNodes(d) / 10);
    else return radius;
}

function nbEndNodes(n)
{
    nb = 0;    
    if(n.children){
        n.children.forEach(function(c){ 
            nb += nbEndNodes(c); 
        });
    }
    else if(n._children){
        n._children.forEach(function(c){ 
            nb += nbEndNodes(c); 
        });
    }
    else nb++;
    
    return nb;
}

function click(d)
{
    if (d.children){
        d._children = d.children;
        d.children = null;
    } 
    else{
        d.children = d._children;
        d._children = null;
    }
    update(d);
}

function collapse(d){
    if (d.children){
        d._children = d.children;
        d._children.forEach(collapse);
        d.children = null;
    }
}

update(root);
};