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

import java.util.Arrays;
import java.util.List;

public class ResultManager {
    private final List<ResultHandler> resultHandlers;
    private final ResultUtil util;
    private final PherfConstants.RunMode runMode;


    public ResultManager(String fileNameSeed, PherfConstants.RunMode runMode) {
        this(runMode, Arrays.asList(
                new XMLResultHandler(fileNameSeed, ResultFileDetails.XML),
                new ImageResultHandler(fileNameSeed, ResultFileDetails.IMAGE),
						new CSVResultHandler(
								fileNameSeed,
								runMode == RunMode.PERFORMANCE ? ResultFileDetails.CSV_DETAILED_PERFORMANCE
										: ResultFileDetails.CSV_DETAILED_FUNCTIONAL),
                new CSVResultHandler(fileNameSeed, ResultFileDetails.CSV_AGGREGATE_PERFORMANCE)
        ));
    }

    public ResultManager(PherfConstants.RunMode runMode, List<ResultHandler> resultHandlers) {
        this.resultHandlers = resultHandlers;
        util = new ResultUtil();
        this.runMode = runMode;
    }

    /**
     * Write out the result to each writer in the pool
     *
     * @param result {@link DataModelResult}
     * @throws Exception
     */
    public synchronized void write(DataModelResult result) throws Exception {
        try {
            util.ensureBaseResultDirExists();
            final DataModelResult dataModelResultCopy = new DataModelResult(result);
            for (ResultHandler handler : resultHandlers) {
                util.write(handler, dataModelResultCopy, runMode);
            }
        } finally {
            for (ResultHandler handler : resultHandlers) {
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
     * Write a combined set of results for each result in the list.
     * @param dataModelResults List<{@link DataModelResult > </>}
     * @throws Exception
     */
    public synchronized void write(List<DataModelResult> dataModelResults) throws Exception {
        util.ensureBaseResultDirExists();

        CSVResultHandler detailsCSVWriter = null;
        try {
            detailsCSVWriter = new CSVResultHandler(PherfConstants.COMBINED_FILE_NAME, ResultFileDetails.CSV_DETAILED_PERFORMANCE);
            for (DataModelResult dataModelResult : dataModelResults) {
                util.write(detailsCSVWriter, dataModelResult, runMode);
            }
        } finally {
            if (detailsCSVWriter != null) {
                detailsCSVWriter.flush();
                detailsCSVWriter.close();
            }
        }
    }
}
