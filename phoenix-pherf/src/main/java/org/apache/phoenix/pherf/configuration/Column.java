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

import org.apache.phoenix.pherf.rules.DataValue;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import java.util.List;
import java.util.Objects;

public class Column {
	private String name;
    private String prefix;
    private DataSequence dataSequence;
    private int length, precision;
    private long minValue, maxValue;
    private int nullChance;
    private boolean userDefined;
    private List<DataValue> dataValues;
	private DataTypeMapping type;
    private boolean useCurrentDate;

    public Column() {
        super();
        // Initialize int to negative value so we can distinguish 0 in mutations
        // Object fields can be detected with null
        this.length = Integer.MIN_VALUE;
        this.minValue = Long.MIN_VALUE;
        this.maxValue = Long.MIN_VALUE;
        this.precision = Integer.MIN_VALUE;
        this.nullChance = Integer.MIN_VALUE;
        this.userDefined = false;
        this.useCurrentDate = false;
    }

    public Column(Column column) {
        this();
        this.type = column.type;
        this.mutate(column);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.type);
    }

    /**
     * Equal if column name and type match
     * @param column
     * @return
     */
    @Override
    public boolean equals(Object column) {
        if (!(column instanceof Column)) {
            return false;
        }
        Column col = (Column)column;
        return (getType() == col.getType());
    }

    public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public DataSequence getDataSequence() {
		return dataSequence;
	}

	public void setDataSequence(DataSequence dataSequence) {
		this.dataSequence = dataSequence;
	}

	public int getLength() {
		return length;
	}
	
	public int getLengthExcludingPrefix() {
		return (this.getPrefix() == null) ? this.length : this.length - this.getPrefix().length();
	}

	public void setLength(int length) {
		this.length = length;
	}

	public DataTypeMapping getType() {
		return type;
	}

	public void setType(DataTypeMapping type) {
		this.type = type;
	}

    public long getMinValue() {
        return minValue;
    }

    public void setMinValue(long minValue) {
        this.minValue = minValue;
    }

    public long getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(long maxValue) {
        this.maxValue = maxValue;
    }

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }

    public void setUseCurrentDate(boolean useCurrentDate) { this.useCurrentDate = useCurrentDate; }

    public boolean getUseCurrentDate() { return useCurrentDate; }

    /**
     * Changes fields of this object to match existing fields from the passed Column
     * null object members are ignored.
     *
     * Field type cannot be mutated.
     * @param column {@link Column}
     *               obj contains only the fields you want to mutate this object into.
     */
    public void mutate(Column column) {
        if (column.getMinValue() != Long.MIN_VALUE) {
            setMinValue(column.getMinValue());
        }

        if (column.getMaxValue() != Long.MIN_VALUE) {
            setMaxValue(column.getMaxValue());
        }

        if (column.getLength() != Integer.MIN_VALUE) {
            setLength(column.getLength());
        }

        if (column.getName() != null) {
            setName(column.getName());
        }

        if (column.getPrefix() != null) {
            setPrefix(column.getPrefix());
        }

        if (column.getDataSequence() != null) {
            setDataSequence(column.getDataSequence());
        }

        if (column.getNullChance() != Integer.MIN_VALUE) {
            setNullChance(column.getNullChance());
        }

        if (column.getPrecision() != Integer.MIN_VALUE) {
            setPrecision(column.getPrecision());
        }

        if (column.isUserDefined()) {
            setUserDefined(column.isUserDefined());
        }

        if (column.dataValues != null) {
           setDataValues(column.getDataValues());
        }

        if(column.getUseCurrentDate()) {
            setUseCurrentDate(column.getUseCurrentDate());
        }
    }

    public int getNullChance() {
        return nullChance;
    }

    public void setNullChance(int nullChance) {
        this.nullChance = nullChance;
    }

    public boolean isUserDefined() {
        return userDefined;
    }

    public void setUserDefined(boolean userDefined) {
        this.userDefined = userDefined;
    }

    public List<DataValue> getDataValues() {
        return dataValues;
    }

    @XmlElementWrapper(name = "valuelist")
    @XmlElement(name = "datavalue")
    public void setDataValues(List<DataValue> dataValues) {
        this.dataValues = dataValues;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }
}