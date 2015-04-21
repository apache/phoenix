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

import org.apache.phoenix.pherf.PherfConstants;
import org.apache.phoenix.pherf.PherfConstants.RunMode;
import org.apache.phoenix.pherf.result.file.ResultFileDetails;
import org.apache.phoenix.pherf.result.impl.CSVResultHandler;
import org.apache.phoenix.pherf.result.impl.ImageResultHandler;
import org.apache.phoenix.pherf.result.impl.XMLResultHandler;
import org.apache.phoenix.pherf.util.PhoenixUtil;

import java.io.*;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.List;

public class ResultUtil {

    /*This variable needs to be static - Otherwise multiple result files will be generated*/
    private static String FILE_SUFFIX = null;

    /**
     * Overload for write all results type to file
     * <p/>
     * TODO Remove when we are sure results are stable. Currently there are no more references to this.
     *
     * @param dataModelResult
     * @param fileName
     * @throws javax.xml.bind.JAXBException
     * @throws IOException
     */
    public synchronized void writeResultToFile(DataModelResult dataModelResult, String fileName, RunMode runMode) throws Exception {

        ResultHandler detailsCSVHandler;
        ResultHandler aggregateCSVHandler;
        ResultHandler xmlResultHandler;
        ResultHandler imageResultHandler;
        List<ResultHandler> handlers = new ArrayList<>();
        try {
            ensureBaseResultDirExists();
            final DataModelResult dataModelResultCopy = new DataModelResult(dataModelResult);

            detailsCSVHandler = new CSVResultHandler(fileName, ResultFileDetails.CSV_DETAILED_PERFORMANCE);
            handlers.add(detailsCSVHandler);
            xmlResultHandler = new XMLResultHandler(fileName, ResultFileDetails.XML);
            handlers.add(xmlResultHandler);
            aggregateCSVHandler = new CSVResultHandler(fileName, ResultFileDetails.CSV_AGGREGATE_PERFORMANCE);
            handlers.add(aggregateCSVHandler);
            imageResultHandler = new ImageResultHandler(fileName, ResultFileDetails.IMAGE);
            handlers.add(imageResultHandler);

            // XML results
            write(xmlResultHandler, dataModelResultCopy, runMode);
            // JPG result visualization
            write(imageResultHandler, dataModelResultCopy, runMode);
            // CSV results
            write(aggregateCSVHandler, dataModelResultCopy, runMode);
            // CSV results details
            write(detailsCSVHandler, dataModelResultCopy, runMode);

        } finally {
            for (ResultHandler handler : handlers) {
                try {
                    if (handler != null) {
                        handler.flush();
                        handler.close();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

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
                writer = new CSVResultHandler("Data_Load_Details", ResultFileDetails.CSV);
                for (WriteThreadTime writeThreadTime : dataLoadThreadTime.getThreadTime()) {
                    List<ResultValue> rowValues = new ArrayList<>();
                    rowValues.add(new ResultValue(PhoenixUtil.getZookeeper()));
                    rowValues.addAll(writeThreadTime.getCsvRepresentation(this));
                    Result result = new Result(ResultFileDetails.CSV_DETAILED_PERFORMANCE, "ZK," + dataLoadThreadTime.getCsvTitle(), rowValues);
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
        ResultFileDetails resultFileDetails = ResultFileDetails.CSV_AGGREGATE_DATA_LOAD;
        try {
            writer = new CSVResultHandler("Data_Load_Summary", ResultFileDetails.CSV);
            for (TableLoadTime loadTime : dataLoadTime.getTableLoadTime()) {
                List<ResultValue> rowValues = new ArrayList<>();
                rowValues.add(new ResultValue(PhoenixUtil.getZookeeper()));
                rowValues.addAll(loadTime.getCsvRepresentation(this));
                Result result = new Result(resultFileDetails, resultFileDetails.getHeader().toString(), rowValues);
                writer.write(result);
            }
        } finally {
            if (writer != null) {
                writer.flush();
                writer.close();
            }
        }
    }

    // TODO remove when stable. There are no more references to this method.
    public synchronized void write(List<DataModelResult> dataModelResults, RunMode runMode) throws Exception {
        ensureBaseResultDirExists();

        CSVResultHandler detailsCSVWriter = null;
        try {
            detailsCSVWriter = new CSVResultHandler(PherfConstants.COMBINED_FILE_NAME, ResultFileDetails.CSV_DETAILED_PERFORMANCE);
            for (DataModelResult dataModelResult : dataModelResults) {
                write(detailsCSVWriter, dataModelResult, runMode);
            }
        } finally {
            if (detailsCSVWriter != null) {
                detailsCSVWriter.flush();
                detailsCSVWriter.close();
            }
        }
    }

    public synchronized void write(ResultHandler resultHandler, DataModelResult dataModelResult, RunMode runMode) throws Exception {
        ResultFileDetails resultFileDetails = resultHandler.getResultFileDetails();
        switch (resultFileDetails) {
            case CSV_AGGREGATE_PERFORMANCE:
            case CSV_DETAILED_PERFORMANCE:
            case CSV_DETAILED_FUNCTIONAL:
                List<List<ResultValue>> rowDetails = getCSVResults(dataModelResult, resultFileDetails, runMode);
                for (List<ResultValue> row : rowDetails) {
                    Result result = new Result(resultFileDetails, resultFileDetails.getHeader().toString(), row);
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
        ensureBaseDirExists(PherfConstants.RESULT_DIR);
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

    public String getSuffix() {
        if (null == FILE_SUFFIX) {
            Date date = new Date();
            Format formatter = new SimpleDateFormat("YYYY-MM-dd_hh-mm-ss");
            FILE_SUFFIX = "_" + formatter.format(date);
        }
        return FILE_SUFFIX;
    }

    public String convertNull(String str) {
        if ((str == null) || str.equals("")) {
            return "null";
        }
        return str;
    }

    private List<List<ResultValue>> getCSVResults(DataModelResult dataModelResult, ResultFileDetails resultFileDetails, RunMode runMode) {
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
                            List<List<ResultValue>> detailedRows = queryResult.getCsvDetailedRepresentation(this, runMode);
                            for (List<ResultValue> detailedRowList : detailedRows) {
                                List<ResultValue> valueList = new ArrayList<>();
                                valueList.add(new ResultValue(convertNull(result.getTableName())));
                                valueList.add(new ResultValue(convertNull(result.getName())));
                                valueList.add(new ResultValue(convertNull(dataModelResult.getZookeeper())));
                                valueList.add(new ResultValue(convertNull(String.valueOf(result.getRowCount()))));
                                valueList.add(new ResultValue(convertNull(String.valueOf(querySetResult.getNumberOfExecutions()))));
                                valueList.add(new ResultValue(convertNull(String.valueOf(querySetResult.getExecutionType()))));
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
            if (!firstPartialSeparator)
                sb.append("|");
            firstPartialSeparator = false;
            sb.append(entry.getKey() + "=" + entry.getValue());
        }
        return sb.toString();
    }
}