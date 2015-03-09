/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.phoenix.pherf.result.impl;

import org.apache.phoenix.pherf.PherfConstants;
import org.apache.phoenix.pherf.result.file.ResultFileDetails;
import org.apache.phoenix.pherf.result.*;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.renderer.xy.StandardXYItemRenderer;
import org.jfree.data.time.Millisecond;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.util.List;

public class ImageResultHandler implements ResultHandler {
    private final String resultFileName;
    private final ResultFileDetails resultFileDetails;

    public ImageResultHandler(String resultFileName, ResultFileDetails resultFileDetails) {
        this(resultFileName, resultFileDetails, true);
    }

    public ImageResultHandler(String resultFileName, ResultFileDetails resultFileDetails, boolean generateFullFileName) {
        ResultUtil util = new ResultUtil();
        this.resultFileName = generateFullFileName ?
                PherfConstants.RESULT_DIR + PherfConstants.PATH_SEPARATOR
                        + PherfConstants.RESULT_PREFIX
                        + resultFileName + util.getSuffix()
                        + resultFileDetails.getExtension().toString()
                : resultFileName;
        this.resultFileDetails = resultFileDetails;
    }

    @Override
    public synchronized void write(Result result) throws Exception {
        TimeSeriesCollection timeSeriesCollection = new TimeSeriesCollection();
        int rowCount = 0;
        int maxLegendCount = 20;
        int chartDimension = 1100;

        ResultValue<DataModelResult> resultValue = result.getResultValues().get(0);
        DataModelResult dataModelResult = resultValue.getResultValue();

        for (ScenarioResult scenarioResult : dataModelResult.getScenarioResult()) {
            for (QuerySetResult querySetResult : scenarioResult.getQuerySetResult()) {
                for (QueryResult queryResult : querySetResult.getQueryResults()) {
                    for (ThreadTime tt : queryResult.getThreadTimes()) {
                        TimeSeries timeSeries = new TimeSeries(queryResult.getStatement() + " :: " + tt.getThreadName());
                        rowCount++;
                        synchronized (tt.getRunTimesInMs()) {
                            for (RunTime rt : tt.getRunTimesInMs()) {
                                if (rt.getStartTime() != null) {
                                    timeSeries.add(new Millisecond(rt.getStartTime()), rt.getElapsedDurationInMs());
                                }
                            }
                        }
                        timeSeriesCollection.addSeries(timeSeries);
                    }
                }
            }
        }
        boolean legend = rowCount > maxLegendCount ? false : true;
        JFreeChart chart = ChartFactory.createTimeSeriesChart(dataModelResult.getName()
                , "Time", "Query Time (ms)", timeSeriesCollection,
                legend, true, false);
        StandardXYItemRenderer renderer = new StandardXYItemRenderer(StandardXYItemRenderer.SHAPES_AND_LINES);
        chart.getXYPlot().setRenderer(renderer);
        chart.getXYPlot().setBackgroundPaint(Color.WHITE);
        chart.getXYPlot().setRangeGridlinePaint(Color.BLACK);
        for (int i = 0; i < rowCount; i++) {
            chart.getXYPlot().getRenderer().setSeriesStroke(i, new BasicStroke(3f));
        }
        try {
            ChartUtilities.saveChartAsJPEG(new File(resultFileName), chart, chartDimension, chartDimension);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public synchronized void flush() throws Exception {

    }

    @Override
    public synchronized void close() throws Exception {

    }

    @Override
    public List<Result> read() throws Exception {
        return null;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public ResultFileDetails getResultFileDetails() {
        return resultFileDetails;
    }
}
