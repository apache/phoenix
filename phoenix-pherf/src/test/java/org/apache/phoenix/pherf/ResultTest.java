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

package org.apache.phoenix.pherf;

import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.phoenix.pherf.PherfConstants;
import org.apache.phoenix.pherf.PherfConstants.RunMode;
import org.apache.phoenix.pherf.jmx.MonitorManager;

import org.apache.phoenix.pherf.result.file.Extension;
import org.apache.phoenix.pherf.result.file.ResultFileDetails;
import org.apache.phoenix.pherf.result.impl.CSVResultHandler;
import org.apache.phoenix.pherf.result.impl.XMLResultHandler;
import org.apache.phoenix.pherf.result.*;
import org.junit.Test;

import org.apache.phoenix.pherf.configuration.Query;

public class ResultTest {

    @Test
    public void testWriters() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        List<ResultHandler> writers = new ArrayList<>();
//        Monitor monitor = new Monitor(new XMLConfigParser("test.*xml"), 100);
//        Future future = executorService.submit(monitor);

        writers.add(new CSVResultHandler(PherfConstants.MONITOR_FILE_NAME, ResultFileDetails.CSV_DETAILED_PERFORMANCE));

    }

    @Test
    public void testMonitorWriter() throws Exception {
        String[] row = "org.apache.phoenix.pherf:type=PherfWriteThreads,6,Mon Jan 05 15:14:00 PST 2015".split(PherfConstants.RESULT_FILE_DELIMETER);
        ResultHandler resultMonitorWriter = null;
        List<ResultValue> resultValues = new ArrayList<>();
        for (String val : row) {
            resultValues.add(new ResultValue(val));
        }

        try {
            resultMonitorWriter = new CSVResultHandler(PherfConstants.MONITOR_FILE_NAME, ResultFileDetails.CSV_MONITOR);
            Result
                    result = new Result(ResultFileDetails.CSV_MONITOR, ResultFileDetails.CSV_MONITOR.getHeader().toString(), resultValues);
            resultMonitorWriter.write(result);
            resultMonitorWriter.write(result);
            resultMonitorWriter.write(result);
            resultMonitorWriter.flush();
            List<Result> results = resultMonitorWriter.read();
            assertEquals("Results did not contain row.", results.size(), 3);

        } finally {
            if (resultMonitorWriter != null) {
                resultMonitorWriter.flush();
                resultMonitorWriter.close();
            }
        }
    }

    @Test
    public void testMonitorResult() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        MonitorManager monitor = new MonitorManager(100);
        Future future = executorService.submit(monitor);
        List<Result> records = null;
        final int TIMEOUT = 30;

        int ct = 0;
        int max = 30;
        // Wait while we write some rows.
        while (!future.isDone()) {
            Thread.sleep(100);
            if (ct == max) {
                int timer = 0;
                monitor.stop();
                while (monitor.isRunning() && (timer < TIMEOUT)) {
                    System.out.println("Waiting for monitor to finish. Seconds Waited :" + timer);
                    Thread.sleep(1000);
                    timer++;
                }
            }

            ct++;
        }
        executorService.shutdown();
        records = monitor.readResults();

        assertNotNull("Could not retrieve records", records);
        assertEquals("Failed to get correct amount of CSV records.", records.size(), monitor.getRowCount());
        assertFalse("Monitor was not stopped correctly.", monitor.isRunning());
    }

    @Test
    public void testExtensionEnum() {
        assertEquals("Extension did not match", Extension.CSV.toString(), ".csv");
        assertEquals("Extension did not match", Extension.DETAILED_CSV.toString(), "_detail.csv");
    }

    @Test
    public void testResult() throws Exception {
        String filename = "testresult";
        ResultHandler xmlResultHandler = null;
        ResultManager resultManager = new ResultManager(filename, RunMode.PERFORMANCE);

        // write result to file
        DataModelResult dataModelResult = new DataModelResult();
        dataModelResult.setZookeeper("mytestzk");
        ScenarioResult scenarioResult = new ScenarioResult();
        scenarioResult.setTableName("MY_TABLE_NAME");

        // Scenario Name left blank on purpose to test that null values get generated correctly.
        //scenarioResult.setName("MY_TEST_SCENARIO");

        dataModelResult.getScenarioResult().add(scenarioResult);
        scenarioResult.setRowCount(999);
        QuerySetResult querySetResult = new QuerySetResult();
        querySetResult.setConcurrency("50");
        scenarioResult.getQuerySetResult().add(querySetResult);
        Query query = new Query();
        Query query2 = new Query();

        // add some spaces so we test query gets normalized
        query.setQueryGroup("g123");
        query.setTenantId("tennantID123");
        query.setStatement("Select    * \n" + "from    FHA");
        query2.setStatement("Select a, b, c  * \n" + "from    FHA2");
        assertEquals("Expected consecutive spaces to be normalized", "Select * from FHA", query.getStatement());

        QueryResult queryResult = new QueryResult(query);
        QueryResult queryResult2 = new QueryResult(query2);
        querySetResult.getQueryResults().add(queryResult);
        querySetResult.getQueryResults().add(queryResult2);

        ThreadTime tt = new ThreadTime();
        tt.setThreadName("thread1");
        Calendar calendar = Calendar.getInstance();
        Date startTime1 = calendar.getTime();
        RunTime runtime1 = new RunTime(startTime1, 1000L, 10);
        tt.getRunTimesInMs().add(runtime1);
        calendar.add(Calendar.MINUTE, -1);
        RunTime runtime2 = new RunTime(calendar.getTime(), 2000L, 20);
        tt.getRunTimesInMs().add(runtime2);
        calendar.add(Calendar.MINUTE, -1);
        RunTime runtime3 = new RunTime(calendar.getTime(), 3000L, 30);
        tt.getRunTimesInMs().add(runtime3);
        queryResult.getThreadTimes().add(tt);
        queryResult2.getThreadTimes().add(tt);

        //resultUtil.writeResultToFile(dataModelResult, filename, RunMode.PERFORMANCE);
        resultManager.write(dataModelResult);

        // Put some stuff in a combined file
        List<DataModelResult> modelResults = new ArrayList<>();
        modelResults.add(dataModelResult);
        modelResults.add(dataModelResult);
        resultManager.write(modelResults);

        // read result from file
        xmlResultHandler = new XMLResultHandler(filename, ResultFileDetails.XML);
        List<Result> resultList = xmlResultHandler.read();
        ResultValue<DataModelResult> resultValue = resultList.get(0).getResultValues().get(0);
        DataModelResult dataModelResultFromFile = resultValue.getResultValue();

        ScenarioResult scenarioResultFromFile = dataModelResultFromFile.getScenarioResult().get(0);
        QuerySetResult querySetResultFromFile = scenarioResultFromFile.getQuerySetResult().get(0);
        QueryResult queryResultFromFile = querySetResultFromFile.getQueryResults().get(0);
        ThreadTime ttFromFile = queryResultFromFile.getThreadTimes().get(0);

        // thread level verification
        assertEquals(10, (int) ttFromFile.getMinTimeInMs().getElapsedDurationInMs());
        assertEquals(30, (int) ttFromFile.getMaxTimeInMs().getElapsedDurationInMs());
        assertEquals(20, (int) ttFromFile.getAvgTimeInMs());
        // 3rd runtime has the earliest start time, therefore that's what's expected.
        assertEquals(runtime3.getStartTime(), ttFromFile.getStartTime());

        assertEquals(runtime1.getResultRowCount(), ttFromFile.getRunTimesInMs().get(0).getResultRowCount());
        assertEquals(runtime2.getResultRowCount(), ttFromFile.getRunTimesInMs().get(1).getResultRowCount());
        assertEquals(runtime3.getResultRowCount(), ttFromFile.getRunTimesInMs().get(2).getResultRowCount());

        // query result level verification
        assertEquals(10, queryResultFromFile.getAvgMinRunTimeInMs());
        assertEquals(30, queryResultFromFile.getAvgMaxRunTimeInMs());
        assertEquals(20, queryResultFromFile.getAvgRunTimeInMs());
        // 3rd runtime has the earliest start time, therefore that's what's expected.
        assertEquals(runtime3.getStartTime(), queryResultFromFile.getStartTime());
    }
}