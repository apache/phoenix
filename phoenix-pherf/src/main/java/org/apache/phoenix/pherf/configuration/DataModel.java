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

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

@XmlRootElement(name = "datamodel")
public class DataModel {
    private String release;
    private String name;
    private List<Scenario> scenarios;
    private List<Column> dataMappingColumns;

    public DataModel() {
    }

    public String getRelease() {
        return this.release;
    }

    @XmlAttribute()
    public void setRelease(String release) {
        this.release = release;
    }

    public List<Scenario> getScenarios() {
        return scenarios;
    }

    @XmlElementWrapper(name = "datamapping")
    @XmlElement(name = "column")
    public void setDataMappingColumns(List<Column> dataMappingColumns) {
        this.dataMappingColumns = dataMappingColumns;
    }

    public List<Column> getDataMappingColumns() {
        return dataMappingColumns;
    }

    @XmlElementWrapper(name = "scenarios")
    @XmlElement(name = "scenario")
    public void setScenarios(List<Scenario> scenarios) {
        this.scenarios = scenarios;
    }

	public String getName() {
		return name;
	}

	@XmlAttribute()
	public void setName(String name) {
		this.name = name;
	}
}

