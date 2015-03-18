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

package org.apache.phoenix.pherf.result.file;

public enum ResultFileDetails {
    CSV(Header.EMPTY, Extension.CSV),
    CSV_AGGREGATE_PERFORMANCE(Header.AGGREGATE_PERFORMANCE, Extension.AGGREGATE_CSV),
    CSV_DETAILED_PERFORMANCE(Header.DETAILED_PERFORMANCE, Extension.DETAILED_CSV),
    CSV_DETAILED_FUNCTIONAL(Header.DETAILED_FUNCTIONAL, Extension.DETAILED_CSV),
    CSV_AGGREGATE_DATA_LOAD(Header.AGGREGATE_DATA_LOAD, Extension.CSV),
    CSV_MONITOR(Header.MONITOR, Extension.CSV),
    XML(Header.EMPTY, Extension.XML),
    IMAGE(Header.EMPTY, Extension.VISUALIZATION);

    private Header header;
    private Extension extension;

    private ResultFileDetails(Header header, Extension extension) {
        this.header = header;
        this.extension = extension;
    }

    public Extension getExtension() {
        return extension;
    }

    public Header getHeader() {
        return header;
    }
}
