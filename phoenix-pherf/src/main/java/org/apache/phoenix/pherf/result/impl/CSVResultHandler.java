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
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.phoenix.pherf.result.Result;
import org.apache.phoenix.pherf.result.ResultHandler;
import org.apache.phoenix.pherf.result.ResultUtil;
import org.apache.phoenix.pherf.result.ResultValue;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * TODO Doc this class. Note that each instance that has a non unique file name will overwrite the last
 */
public class CSVResultHandler implements ResultHandler {

    private final ResultUtil util;
    private final ResultFileDetails resultFileDetails;
    private final String resultFileName;
    private volatile CSVPrinter csvPrinter = null;
    private volatile boolean isClosed = true;

    public CSVResultHandler(String resultFileName, ResultFileDetails resultFileDetails) {
        this(resultFileName, resultFileDetails, true);
    }

    public CSVResultHandler(String resultFileName, ResultFileDetails resultFileDetails, boolean generateFullFileName) {
        this.util = new ResultUtil();
        this.resultFileName = generateFullFileName ?
                PherfConstants.RESULT_DIR + PherfConstants.PATH_SEPARATOR
                        + PherfConstants.RESULT_PREFIX
                        + resultFileName + util.getSuffix()
                        + resultFileDetails.getExtension().toString()
            : resultFileName;
        this.resultFileDetails = resultFileDetails;
    }

    @Override
    public synchronized void write(Result result) throws IOException {
        util.ensureBaseResultDirExists();

        open(result);
        csvPrinter.printRecord(result.getResultValues());
        flush();
    }

    @Override
    public synchronized void flush() throws IOException {
        if (csvPrinter != null) {
            csvPrinter.flush();
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (csvPrinter != null) {
            csvPrinter.flush();
            csvPrinter.close();
            isClosed = true;
        }
    }

    @Override
    public synchronized List<Result> read() throws IOException {
        CSVParser parser = null;
        util.ensureBaseResultDirExists();
        try {
            File file = new File(resultFileName);
            parser = CSVParser.parse(file, Charset.defaultCharset(), CSVFormat.DEFAULT);
            List<CSVRecord> records = parser.getRecords();
            List<Result> results = new ArrayList<>();
            String header = null;
            for (CSVRecord record : records) {

                // First record is the CSV Header
                if (record.getRecordNumber() == 1) {
                    header = record.toString();
                    continue;
                }
                List<ResultValue> resultValues = new ArrayList<>();
                for (String val : record.toString().split(PherfConstants.RESULT_FILE_DELIMETER)) {
                    resultValues.add(new ResultValue(val));
                }
                Result result = new Result(resultFileDetails, header, resultValues);
                results.add(result);
            }
            return results;
        } finally {
            parser.close();
        }
    }

    private void open(Result result) throws IOException {
        // Check if already so we only open one writer
        if (csvPrinter != null) {
            return;
        }
        csvPrinter = new CSVPrinter(new PrintWriter(resultFileName), CSVFormat.DEFAULT);
        csvPrinter.printRecord(result.getHeader().split(PherfConstants.RESULT_FILE_DELIMETER));
        isClosed = false;
    }

    @Override
    public synchronized boolean isClosed() {
        return isClosed;
    }

    @Override
    public ResultFileDetails getResultFileDetails() {
        return resultFileDetails;
    }
}
