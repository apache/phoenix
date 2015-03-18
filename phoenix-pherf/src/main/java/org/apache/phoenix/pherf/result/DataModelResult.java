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

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.phoenix.pherf.configuration.DataModel;

@XmlRootElement(namespace = "org.apache.phoenix.pherf.result")
public class DataModelResult extends DataModel {
	private List<ScenarioResult> scenarioResult = new ArrayList<ScenarioResult>();
	private String zookeeper;

	public List<ScenarioResult> getScenarioResult() {
		return scenarioResult;
	}

	public void setScenarioResult(List<ScenarioResult> scenarioResult) {
		this.scenarioResult = scenarioResult;
	}
	
	public DataModelResult() {
	}

    private DataModelResult(String name, String release, String zookeeper) {
        this.setName(name);
        this.setRelease(release);
        this.zookeeper = zookeeper;
    }

    /**
     * Copy constructor
     * 
     * @param dataModelResult
     */
    public DataModelResult(DataModelResult dataModelResult) {
        this(dataModelResult.getName(), dataModelResult.getRelease(), dataModelResult.getZookeeper());
        this.scenarioResult = dataModelResult.getScenarioResult();
    }
	
	public DataModelResult(DataModel dataModel, String zookeeper) {
	    this(dataModel.getName(), dataModel.getRelease(), zookeeper);
	}
	
	public DataModelResult(DataModel dataModel) {
		this(dataModel, null);
	}

	@XmlAttribute()
	public String getZookeeper() {
		return zookeeper;
	}

	public void setZookeeper(String zookeeper) {
		this.zookeeper = zookeeper;
	}
}
