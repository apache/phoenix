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

public class DataLoadTimeSummary {
	private List<TableLoadTime> tableLoadTime = new ArrayList<TableLoadTime>();

	public List<TableLoadTime> getTableLoadTime() {
		return tableLoadTime;
	}
	
	public void add(String tableName, int rowCount, int durationInMs) {
		tableLoadTime.add(new TableLoadTime(tableName, rowCount, durationInMs));
	}

	public void setTableLoadTime(List<TableLoadTime> tableLoadTime) {
		this.tableLoadTime = tableLoadTime;
	}

}

class TableLoadTime {
	private int durationInMs;
	private String tableName;
	private int rowCount;

	public TableLoadTime(String tableName, int rowCount, int durationInMs) {
		this.tableName = tableName;
		this.rowCount = rowCount;
		this.durationInMs = durationInMs;
	}
	
	public List<ResultValue> getCsvRepresentation(ResultUtil util) {
        List<ResultValue> rowValues = new ArrayList<>();
        rowValues.add(new ResultValue(util.convertNull(getTableName())));
        rowValues.add(new ResultValue(util.convertNull(String.valueOf(getRowCount()))));
        rowValues.add(new ResultValue(util.convertNull(String.valueOf(getDurationInMs()))));

        return rowValues;
    }

	public int getDurationInMs() {
		return durationInMs;
	}

	public void setDurationInMs(int durationInMs) {
		this.durationInMs = durationInMs;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public int getRowCount() {
		return rowCount;
	}

	public void setRowCount(int rowCount) {
		this.rowCount = rowCount;
	}
}
