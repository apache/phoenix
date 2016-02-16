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

package org.apache.phoenix.pherf.result;

import org.apache.commons.io.FileUtils;
import org.apache.phoenix.pherf.PherfConstants;
import org.apache.phoenix.pherf.result.file.ResultFileDetails;
import org.apache.phoenix.pherf.result.impl.CSVFileResultHandler;
import org.apache.phoenix.pherf.result.impl.CSVResultHandler;
import org.apache.phoenix.pherf.util.PhoenixUtil;

import java.io.File;
import java.io.IOException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class ResultUtil {

    /*This variable needs to be static - Otherwise multiple result files will be generated*/
    private static String FILE_SUFFIX = null;

    /**
     * Write data load time details
     *
     * @param dataLoadThreadTime {@link DataLoadThreadTime}
     * @throws IOException
     */
    public synchronized void write(DataLoadThreadTime dataLoadThreadTime) throws IOException {
        ensureBaseResultDirExists();

        CSVResultHandler writer = null;
        try {
            if (!dataLoadThreadTime.getThreadTime().isEmpty()) {
                writer = new CSVFileResultHandler();
                writer.setResultFileDetails(ResultFileDetails.CSV);
                writer.setResultFileName("Data_Load_Details");

                for (WriteThreadTime writeThreadTime : dataLoadThreadTime.getThreadTime()) {
                    List<ResultValue> rowValues = new ArrayList<>();
                    rowValues.add(new ResultValue(PhoenixUtil.getZookeeper()));
                    rowValues.addAll(writeThreadTime.getCsvRepresentation(this));
                    Result
                            result =
                            new Result(ResultFileDetails.CSV_DETAILED_PERFORMANCE,
                                    "ZK," + dataLoadThreadTime.getCsvTitle(), rowValues);
                    writer.write(result);
                }
            }
        } finally {
            if (writer != null) {
                writer.flush();
                writer.close();
            }
        }
    }

    /**
     * Write data load time summary
     *
     * @param dataLoadTime
     * @throws IOException
     */
    public synchronized void write(DataLoadTimeSummary dataLoadTime) throws IOException {
        ensureBaseResultDirExists();

        CSVResultHandler writer = null;
        ResultFileDetails resultFileDetails;
        if (PhoenixUtil.isThinDriver()) {
            resultFileDetails = ResultFileDetails.CSV_THIN_AGGREGATE_DATA_LOAD;
        } else {
            resultFileDetails = ResultFileDetails.CSV_AGGREGATE_DATA_LOAD;
        }
        try {
            writer = new CSVFileResultHandler();
            writer.setResultFileDetails(resultFileDetails);
            writer.setResultFileName("Data_Load_Summary");

            for (TableLoadTime loadTime : dataLoadTime.getTableLoadTime()) {
                List<ResultValue> rowValues = new ArrayList<>();
                if (PhoenixUtil.isThinDriver()) {
                    rowValues.add(new ResultValue(PhoenixUtil.getQueryServerUrl()));
                } else {
                    rowValues.add(new ResultValue(PhoenixUtil.getZookeeper()));
                }
                rowValues.addAll(loadTime.getCsvRepresentation(this));
                Result
                        result =
                        new Result(resultFileDetails, resultFileDetails.getHeader().toString(),
                                rowValues);
                writer.write(result);
            }
        } finally {
            if (writer != null) {
                writer.flush();
                writer.close();
            }
        }
    }

    public synchronized void write(ResultHandler resultHandler, DataModelResult dataModelResult)
            throws Exception {
        ResultFileDetails resultFileDetails = resultHandler.getResultFileDetails();
        switch (resultFileDetails) {
        case CSV_AGGREGATE_PERFORMANCE:
        case CSV_DETAILED_PERFORMANCE:
        case CSV_DETAILED_FUNCTIONAL:
            List<List<ResultValue>>
                    rowDetails =
                    getCSVResults(dataModelResult, resultFileDetails);
            for (List<ResultValue> row : rowDetails) {
                Result
                        result =
                        new Result(resultFileDetails, resultFileDetails.getHeader().toString(),
                                row);
                resultHandler.write(result);
            }
            break;
        default:
            List<ResultValue> resultValue = new ArrayList();
            resultValue.add(new ResultValue<>(dataModelResult));
            resultHandler.write(new Result(resultFileDetails, null, resultValue));
            break;
        }
    }

    public void ensureBaseResultDirExists() {
        PherfConstants constants = PherfConstants.create();
        String resultDir = constants.getProperty("pherf.default.results.dir");
        ensureBaseDirExists(resultDir);
    }

    /**
     * Utility method to check if base result dir exists
     */
    public void ensureBaseDirExists(String directory) {
        File baseDir = new File(directory);
        if (!baseDir.exists()) {
            baseDir.mkdir();
        }
    }
    
    /**
     * Utility method to delete directory
     * @throws IOException 
     */
    public void deleteDir(String directory) throws IOException {
        File baseDir = new File(directory);
        if (baseDir.exists()) {
            FileUtils.deleteDirectory(baseDir);
        }
    }

    public String getSuffix() {
        if (null == FILE_SUFFIX) {
            Date date = new Date();
            Format formatter = new SimpleDateFormat("YYYY-MM-dd_hh-mm-ss");
            FILE_SUFFIX = formatter.format(date);
        }
        return "_" + FILE_SUFFIX;
    }

    public String convertNull(String str) {
        if ((str == null) || str.equals("")) {
            return "null";
        }
        return str;
    }

    /**
     * Used by custom ResultWriter out Pherf's normal code base
     *
     * @return Header field as a {@link Result}
     */
    @SuppressWarnings("unused")
    public Result getCSVHeaderAsResult(String row) {
        List<ResultValue> resultValues = new ArrayList<>();
        resultValues.add(new ResultValue(row));
        return new Result(ResultFileDetails.CSV, row, resultValues);
    }

    private List<List<ResultValue>> getCSVResults(DataModelResult dataModelResult,
            ResultFileDetails resultFileDetails) {
        List<List<ResultValue>> rowList = new ArrayList<>();

        for (ScenarioResult result : dataModelResult.getScenarioResult()) {
            for (QuerySetResult querySetResult : result.getQuerySetResult()) {
                for (QueryResult queryResult : querySetResult.getQueryResults()) {
                    switch (resultFileDetails) {
                    case CSV_AGGREGATE_PERFORMANCE:
                        List<ResultValue> csvResult = queryResult.getCsvRepresentation(this);
                        rowList.add(csvResult);
                        break;
                    case CSV_DETAILED_PERFORMANCE:
                    case CSV_DETAILED_FUNCTIONAL:
                        List<List<ResultValue>>
                                detailedRows =
                                queryResult.getCsvDetailedRepresentation(this, resultFileDetails);
                        for (List<ResultValue> detailedRowList : detailedRows) {
                            List<ResultValue> valueList = new ArrayList<>();
                            valueList.add(new ResultValue(convertNull(result.getTableName())));
                            valueList.add(new ResultValue(convertNull(result.getName())));
                            valueList.add(new ResultValue(
                                    convertNull(dataModelResult.getZookeeper())));
                            valueList.add(new ResultValue(
                                    convertNull(String.valueOf(result.getRowCount()))));
                            valueList.add(new ResultValue(convertNull(
                                    String.valueOf(querySetResult.getNumberOfExecutions()))));
                            valueList.add(new ResultValue(convertNull(
                                    String.valueOf(querySetResult.getExecutionType()))));
                            if (result.getPhoenixProperties() != null) {
                                String props = buildProperty(result);
                                valueList.add(new ResultValue(convertNull(props)));
                            } else {
                                valueList.add(new ResultValue("null"));
                            }
                            valueList.addAll(detailedRowList);
                            rowList.add(valueList);
                        }
                        break;
                    default:
                        break;
                    }
                }
            }
        }
        return rowList;
    }

    private String buildProperty(ScenarioResult result) {
        StringBuffer sb = new StringBuffer();
        boolean firstPartialSeparator = true;

        for (Map.Entry<String, String> entry : result.getPhoenixProperties().entrySet()) {
            if (!firstPartialSeparator) sb.append("|");
            firstPartialSeparator = false;
            sb.append(entry.getKey() + "=" + entry.getValue());
        }
        return sb.toString();
    }
    
    /**
     * Set the file suffix
     * @param suffix
     */
    public static void setFileSuffix(String suffix) {
    	FILE_SUFFIX = suffix;
    }
}