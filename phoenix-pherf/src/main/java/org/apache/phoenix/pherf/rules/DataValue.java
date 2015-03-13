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

package org.apache.phoenix.pherf.rules;

import org.apache.phoenix.pherf.configuration.DataTypeMapping;

import javax.xml.bind.annotation.*;

public class DataValue {
    private DataTypeMapping type;
    private String value;
    private String maxValue;
    private String minValue;
    private int distribution;

    public DataValue() {
        super();
    }

    public DataValue(DataTypeMapping type, String value) {
        this.type = type;
        this.value = value;
        this.distribution = Integer.MIN_VALUE;
    }

    public DataValue(DataValue dataValue) {
        this(dataValue.getType(), dataValue.getValue());
        this.setDistribution(dataValue.getDistribution());
        this.setMinValue(dataValue.getMinValue());
        this.setMaxValue(dataValue.getMaxValue());
    }

    public String getValue() {
        return value;
    }

    public DataTypeMapping getType() {
        return type;
    }

    public int getDistribution() {
        return distribution;
    }

    @XmlAttribute()
    public void setDistribution(int distribution) {
        this.distribution = distribution;
    }

    public void setType(DataTypeMapping type) {
        this.type = type;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getMinValue() {
        return minValue;
    }

    public void setMinValue(String minValue) {
        this.minValue = minValue;
    }

    public String getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(String maxValue) {
        this.maxValue = maxValue;
    }
}
