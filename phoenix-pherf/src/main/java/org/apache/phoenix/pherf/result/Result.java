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

import org.apache.phoenix.pherf.result.file.ResultFileDetails;

import java.util.List;

/**
 * Common container for Pherf results.
 */
public class Result {
    private final List<ResultValue> resultValues;
    private final ResultFileDetails type;
    private final String header;

    /**
     *
     * @param type {@link org.apache.phoenix.pherf.result.file.ResultFileDetails} Currently unused, but gives metadata about the
     *                                                           contents of the result.
     * @param header Used for CSV, otherwise pass null. For CSV pass comma separated string of header fields.
     * @param messageValues List<{@link ResultValue} All fields combined represent the data
     *                      for a row to be written.
     */
    public Result(ResultFileDetails type, String header, List<ResultValue> messageValues) {
        this.resultValues = messageValues;
        this.header = header;
        this.type = type;
    }

    public List<ResultValue> getResultValues() {
        return resultValues;
    }

    public String getHeader() {
        return header;
    }
}
