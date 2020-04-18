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

import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.pherf.result.Result;
import org.apache.phoenix.pherf.result.ResultValue;
import org.apache.phoenix.pherf.result.file.ResultFileDetails;
import org.apache.phoenix.pherf.result.impl.CSVFileResultHandler;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

public class PherfMainIT extends ResultBaseTestIT {

    public HashMap<String, String> mapResults(Result r) throws IOException {
        HashMap<String, String> map = new HashMap<>();
        List<ResultValue> resultValues = r.getResultValues();
        String[] headerValues = r.getHeader().split(PherfConstants.RESULT_FILE_DELIMETER);
        for (int i = 0; i < headerValues.length; i++) {
            map.put(StringUtils.strip(headerValues[i],"[] "),
                    StringUtils.strip(resultValues.get(i).toString(), "[] "));
        }
        return map;
    }

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();

    @Test
    public void testPherfMain() throws Exception {
        String[] args = { "-q", "-l",
                "--schemaFile", ".*create_prod_test_unsalted.sql",
                "--scenarioFile", ".*prod_test_unsalted_scenario.xml",
                "-m", "--monitorFrequency", "10" };
        Pherf pherf = new Pherf(args);
        pherf.run();

        // verify that none of the scenarios threw any exceptions
        for (Future<Void> future : pherf.workloadExecutor.jobs.values()) {
            future.get();
        }
    }

    @Test
    public void testQueryTimeout() throws Exception {
        // Timeout of 0 ms means every query execution should time out
        String[] args = {"-q", "-l",
                "-drop", "all",
                "-schemaFile", ".*timeout_test_schema.sql",
                "-scenarioFile", ".*timeout_test_scenario.xml" };
        Pherf p = new Pherf(args);
        p.run();

        CSVFileResultHandler rh = new CSVFileResultHandler();
        rh.setResultFileDetails(ResultFileDetails.CSV_DETAILED_PERFORMANCE);
        rh.setResultFileName("COMBINED");
        List<Result> resultList = rh.read();
        for (Result r : resultList) {
            HashMap<String, String> resultsMap = mapResults(r);
            if (resultsMap.get("QUERY_ID").equals("q1")) {
                assertEquals(resultsMap.get("TIMED_OUT"), "true");
            }
        }
    }

    @Test
    public void testLargeQueryTimeout() throws Exception {
        // Timeout of max_long ms means every query execution should finish without timing out
        String[] args = {"-q", "-l",
                "-drop", "all",
                "-schemaFile", ".*timeout_test_schema.sql",
                "-scenarioFile", ".*timeout_test_scenario.xml" };
        Pherf p = new Pherf(args);
        p.run();

        CSVFileResultHandler rh = new CSVFileResultHandler();
        rh.setResultFileDetails(ResultFileDetails.CSV_DETAILED_PERFORMANCE);
        rh.setResultFileName("COMBINED");
        List<Result> resultList = rh.read();
        for (Result r : resultList) {
            HashMap<String, String> resultsMap = mapResults(r);
            if (resultsMap.get("QUERY_ID").equals("q2")) {
                assertEquals(resultsMap.get("TIMED_OUT"), "false");
            }
        }
    }

    @Test
    public void testNoQueryTimeout() throws Exception {
        // Missing timeout attribute means every query execution should finish without timing out
        String[] args = {"-q", "-l",
                "-drop", "all",
                "-schemaFile", ".*timeout_test_schema.sql",
                "-scenarioFile", ".*timeout_test_scenario.xml" };
        Pherf p = new Pherf(args);
        p.run();

        CSVFileResultHandler rh = new CSVFileResultHandler();
        rh.setResultFileDetails(ResultFileDetails.CSV_DETAILED_PERFORMANCE);
        rh.setResultFileName("COMBINED");
        List<Result> resultList = rh.read();
        for (Result r : resultList) {
            HashMap<String, String> resultsMap = mapResults(r);
            if (resultsMap.get("QUERY_ID").equals("q3")) {
                assertEquals(resultsMap.get("TIMED_OUT"), "false");
            }
        }
    }

}