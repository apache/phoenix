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

package org.apache.phoenix.pherf.configuration;

import java.sql.Types;

public enum DataTypeMapping {
    VARCHAR("VARCHAR", Types.VARCHAR),
    CHAR("CHAR", Types.CHAR),
    DECIMAL("DECIMAL", Types.DECIMAL),
    INTEGER("INTEGER", Types.INTEGER),
    DATE("DATE", Types.DATE);

    private final String sType;
    private final int dType;

    private DataTypeMapping(String sType, int dType) {
        this.dType = dType;
        this.sType = sType;
    }

    @Override
    public String toString() {
        return this.sType;
    }

    public int getType() {
        return this.dType;
    }
}
