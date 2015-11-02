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

import org.apache.commons.csv.CSVPrinter;
import org.apache.phoenix.pherf.result.Result;
import org.apache.phoenix.pherf.result.ResultUtil;

import java.io.IOException;

public abstract class CSVResultHandler extends DefaultResultHandler {
    protected final ResultUtil util;
    protected volatile CSVPrinter csvPrinter = null;
    protected volatile boolean isClosed = true;

    public CSVResultHandler() {
        this.util = new ResultUtil();
    }

    @Override
    public synchronized void write(Result result) throws IOException {
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
    public synchronized boolean isClosed() {
        return isClosed;
    }

    /**
     * This method is meant to open the connection to the target CSV location
     * @param header {@link String} Comma separated list of header values for CSV
     * @throws IOException
     */
    protected abstract void open(String header) throws IOException;
}
