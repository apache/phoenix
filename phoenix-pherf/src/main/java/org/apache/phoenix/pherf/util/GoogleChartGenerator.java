/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.pherf.util;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.phoenix.pherf.PherfConstants;
import org.apache.phoenix.pherf.PherfConstants.CompareType;
import org.apache.phoenix.pherf.result.file.ResultFileDetails;

/**
 * Compare results based on set threshold and render results as Google Charts
 */
public class GoogleChartGenerator {

	private String[] labels;
	private CompareType compareType;
	private final Map<String, DataNode> datanodes = new TreeMap<String, DataNode>();
	private final PherfConstants constants = PherfConstants.create();
	private final String resultDir = constants.getProperty("pherf.default.results.dir");
	private final double threshold = Double.parseDouble(constants.getProperty("pherf.default.comparison.threshold"));

	public GoogleChartGenerator(String labels, CompareType compareType) {
		this.setLabels(labels);
		this.setCompareType(compareType);
	}
	
	String[] getLabels() {
		return labels;
	}

	void setLabels(String[] labels) {
		this.labels = labels;
	}

	void setLabels(String labels) {
		this.labels = labels.split(",");
	}
	
	CompareType getCompareType() {
		return this.compareType;
	}
	
    void setCompareType(CompareType compareType) {
		this.compareType = compareType;
	}
	
	public void readAndRender() {
		try {
			for (String label : labels) {
				read(label);
			}
			renderAsGoogleChartsHTML();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Reads aggregate file and convert it to DataNode 
	 * @param label
	 * @throws Exception
	 */
    private void read(String label) throws Exception {
		String resultFileName = resultDir 
				+ PherfConstants.PATH_SEPARATOR
				+ PherfConstants.RESULT_PREFIX 
				+ label
				+ ResultFileDetails.CSV_AGGREGATE_PERFORMANCE.getExtension();

    	FileReader in = new FileReader(resultFileName);
    	final CSVParser parser = new CSVParser(in, CSVFormat.DEFAULT.withHeader());

        for (CSVRecord record : parser) {
            String group = record.get("QUERY_GROUP");
            String query = record.get("QUERY");
            String explain = record.get("EXPLAIN_PLAN");
            String tenantId = record.get("TENANT_ID");
            long avgTime = Long.parseLong(record.get("AVG_TIME_MS"));
            long minTime = Long.parseLong(record.get("AVG_MIN_TIME_MS"));
            long numRuns = Long.parseLong(record.get("RUN_COUNT"));
            long rowCount = Long.parseLong(record.get("RESULT_ROW_COUNT"));
            Node node = new Node(minTime, avgTime, numRuns, explain, query, tenantId, label, rowCount);
            
            if (datanodes.containsKey(group)) {
            	datanodes.get(group).getDataSet().put(label, node);
            } else {
            	datanodes.put(group, new DataNode(label, node));
            }
        }
        parser.close();
    }

    /**
     * Verifies if the first result is within the set 
     * threshold of pherf.default.comparison.threshold
     * set in pherf.properties files
     * @param threshold
     * @return
     */
    private boolean verifyWithinThreshold(double threshold) {
    	long resetTimeToCompare = -1;
    	long timeToCompare = resetTimeToCompare;
    	for (Map.Entry<String, DataNode> dn : datanodes.entrySet()) {    	   	
        	for (Map.Entry<String, Node> node : dn.getValue().getDataSet().entrySet()) {
        		if (timeToCompare == -1) {
        			timeToCompare = node.getValue().getTime(getCompareType());
        			if (timeToCompare < 10) { // extremely small query time in ms therefore don't compare
        				return true;
        			}
        		}
				if ((((double) (timeToCompare - node.getValue().getTime(getCompareType()))) / (double) node
						.getValue().getTime(getCompareType())) > threshold) {
					return false;
				}
        	}
        	timeToCompare = resetTimeToCompare;
    	}
    	return true;
    }
    
    /**
     * Render results as Google charts
     * @throws FileNotFoundException
     * @throws UnsupportedEncodingException
     */
    private void renderAsGoogleChartsHTML() throws FileNotFoundException, UnsupportedEncodingException {
    	String lastKeyPrefix = "";
    	StringBuffer sb = new StringBuffer();
    	for (String label : labels) {
    		sb.append("dataTable.addColumn('number', '" + label + "');\n");
    		sb.append("dataTable.addColumn({type: 'string', role: 'tooltip', 'p': {'html': true}});\n");
    	}
    	sb.append("dataTable.addRows([\n");
    	for (Map.Entry<String, DataNode> dn : datanodes.entrySet()) {
    		String currentKeyPrefix = dn.getKey().substring(0, dn.getKey().indexOf('|'));
    		if (!lastKeyPrefix.equalsIgnoreCase(currentKeyPrefix) && lastKeyPrefix != "") {
    			sb.append(getBlankRow());
    		}
    		lastKeyPrefix = currentKeyPrefix;
        	sb.append("['" + dn.getKey() + "'");
        	for (Map.Entry<String, Node> nodeSet : dn.getValue().getDataSet().entrySet()) {
        		sb.append (", " + nodeSet.getValue().getTime(getCompareType()));
        		sb.append (",'" + getToolTipAsHTML(dn.getValue().getDataSet()) + "'");
        	}
        	sb.append("],\n");
        }
    	String summaryFile = PherfConstants.create().getProperty("pherf.default.summary.file");
    	String title = labels[0];
    	PrintWriter writer = new PrintWriter(summaryFile, "UTF-8");
    	
    	writer.println(StaticGoogleChartsRenderingData.HEADER.replace("[title]", title));
    	writer.println(sb.substring(0, sb.length() - 2) + "\n]);");
		String thresholdString = Math.round((threshold*100)) + "%"; 
		String footer = StaticGoogleChartsRenderingData.FOOTER
				.replace("[summary]",
						((verifyWithinThreshold(threshold) == true ? "<font color=green>PASSED | Results are within ": 
							"<font color=red>FAILED | Results are outside "))
								+ "set threshold of " + thresholdString + "</font><br>"
								+ new SimpleDateFormat("yyyy/MM/dd ha z").format(new Date()));
		footer = footer.replace("[title]", title);
    	writer.println(footer);
    	writer.close();
    }
    
    /**
     * Render a blank Google charts row
     * @return
     */
    private String getBlankRow() {
    	String ret = "['" + new String(new char[60]).replace("\0", ".") + "'";
    	for (int i=0; i<labels.length; i++)
    		ret += ",0,''";
    	ret += "],";
    	return ret;
	}

    /**
     * Render tooltip as HTML table
     * @param nodeDataSet
     * @return
     */
	private String getToolTipAsHTML(Map<String, Node> nodeDataSet) {
       	String ret = "<table width=1000 cellpadding=1 cellspacing=0 border=0 bgcolor=#F4F4F4><tr>";
    	for (Map.Entry<String, Node> nodeSet : nodeDataSet.entrySet())	
    		ret += "<td>" + getToolText(nodeSet.getValue()) + "</td>";
    	return ret + "</tr></table>";
    }
    
	/**
	 * Get tooltip for node
	 * @param node
	 * @return
	 */
    private String getToolText(Node node) {
        return node.getLabelAsHTML() 
        		+ node.getAvgTimeAsHTML()
        		+ node.getMinTimeAsHTML()
        		+ node.getNumRunsAsHTML()
        		+ node.getRowCountAsHTML()
        		+ node.getExplainPlanAsHTML()
        		+ node.getQueryAsHTML();
    }
    
	/**
     * DataNode to store results to render and compare 
     */
    class DataNode {
    	private Map<String, Node> dataSet = new LinkedHashMap<String, Node>();
		
    	public DataNode(String label, Node node) {
    		this.getDataSet().put(label, node);
    	}
    	
		public Map<String, Node> getDataSet() {
			return dataSet;
		}
		public void setDataSet(Map<String, Node> dataSet) {
			this.dataSet = dataSet;
		}
    }
    
    class Node {
    	private String explainPlan;
    	private String query;
    	private String tenantId;
    	private long minTime;
    	private long avgTime;
    	private long numRuns;
    	private long rowCount;
    	private String label;
    	private DecimalFormat df = new DecimalFormat("#.#");
    	
    	public Node(long minTime, long avgTime, long numRuns, String explainPlan, String query, String tenantId, String label, long rowCount) {
    		this.setMinTime(minTime);
    		this.setAvgTime(avgTime);
    		this.setNumRuns(numRuns);
    		this.setExplainPlan(explainPlan);
    		this.setQuery(query);
    		this.setTenantId(tenantId);
    		this.setLabel(label);
    		this.setRowCount(rowCount);
    	}
    	
		String getExplainPlan() {
			return explainPlan;
		}
		String getExplainPlanAsHTML() {
			return "</br><font face=arial size=1><b>EXPLAIN PLAN </b>"
					+ explainPlan.replace("'", "") + "</font><br>";
		}
		
		void setExplainPlan(String explainPlan) {
			this.explainPlan = explainPlan;
		}
		long getTime(CompareType compareType) {
			return (compareType == CompareType.AVERAGE ? getAvgTime() : getMinTime());
		}
		
		long getMinTime() {
			if (minTime <= 2) 
				return 2;
			else
				return minTime;
		}
		public String getMinTimeAsHTML() {
			return "<font face=arial size=1><b>MIN TIME </b></font><font face=arial size=3>"
					+ minTime
					+ " ms ("
					+ df.format((double) minTime / 1000)
					+ " sec)</font><br>";
		}
		void setMinTime(long minTime) {
			this.minTime = minTime;
		}
		long getAvgTime() {
			return avgTime;
		}
		public String getAvgTimeAsHTML() {
			return "<font face=arial size=1><b>AVERAGE TIME </b></font><font face=arial size=3>"
					+ avgTime
					+ " ms ("
					+ df.format((double) avgTime / 1000)
					+ " sec)</font><br>";
		}
		void setAvgTime(long avgTime) {
			this.avgTime = avgTime;
		}

		public long getNumRuns() {
			return numRuns;
		}
		public String getNumRunsAsHTML() {
			return "<font face=arial size=1><b>NUMBER OF RUNS </b></font><font face=arial size=3>"
					+ numRuns + "</font><br>";
		}

		public void setNumRuns(long numRuns) {
			this.numRuns = numRuns;
		}

		public String getQuery() {
			return query;
		}

		public String getQueryAsHTML() {
			return "<br><font face=arial size=1><b>QUERY </b>"
					+ query.replace("'", "") + " (TENANT ID: " + getTenantId()
					+ ")</font><br>";
		}
		
		public void setQuery(String query) {
			this.query = query;
		}

		public String getTenantId() {
			return tenantId;
		}
		
		public void setTenantId(String tenantId) {
			this.tenantId = tenantId;
		}

		public String getLabel() {
			return label;
		}
		
		public String getLabelAsHTML() {
			return "<font face=arial size=4 color=#666699>" + label
					+ "</font><br>";
		}

		public void setLabel(String label) {
			this.label = label;
		}

		public long getRowCount() {
			return rowCount;
		}
		public String getRowCountAsHTML() {
			return "<font face=arial size=1><b>RESULT ROW COUNT </b></font><font face=arial size=3>"
					+ rowCount + "</font><br>";
		}

		public void setRowCount(long rowCount) {
			this.rowCount = rowCount;
		}
    }
    
    static class StaticGoogleChartsRenderingData {
		public static String HEADER = "<html><head><title>[title]</title>"
				+ "<script type='text/javascript' src='https://www.google.com/jsapi'></script>"
				+ "<script type='text/javascript'>google.load('visualization', '1', {packages: ['corechart', 'bar']});google.setOnLoadCallback(drawMaterial);"
				+ "function drawMaterial() {var dataTable = new google.visualization.DataTable();dataTable.addColumn('string', 'Query');";
		public static String FOOTER = "var options = {title: '',titleTextStyle: {color: 'gray', fontName: 'Raleway', fontSize: '24'},hAxis: {title: 'Minimum query time for all runs in milli-seconds (ms) | Scaled logrithmic | Hover to see details',titleTextStyle: {italic: false,fontName: 'arial', fontSize: '12'},"
				+ "logScale: true,minValue: 0,textStyle: { fontName: 'arial', fontSize: '14'},},vAxis: {textStyle: {fontName: 'arial', fontSize: '12', fontWidth: 'normal', paddingRight: '100',marginRight: '100'}},"
				+ "chartArea: {left:300, width: 500, right: 400, top: 50, height: 700},"
				+ "legend:{textStyle:{fontSize:'13', fontName:'arial'}},"
				+ "tooltip: {isHtml: true},width: 1200,height: 800,"
				+ "bars:'horizontal',bar: { groupWidth: '75%' },colors: ['#E1A5A9', '#A9A5BC', '#A9A5E1']};"
				+ "var material = new google.visualization.BarChart(document.getElementById('chart_div'));"
				+ "material.draw(dataTable, options);}"
				+ "</script></head><body><b><font face=raleway size=5>PHERFED [title]</font><br><font face=raleway size=4>[summary]</font></b><div id='chart_div' style='margin:0px;padding:0px;'></div></body></html>";
    }
}
