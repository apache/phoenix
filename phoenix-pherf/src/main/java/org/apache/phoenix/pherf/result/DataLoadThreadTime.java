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

import org.apache.phoenix.pherf.PherfConstants;

public class DataLoadThreadTime {
	private List<WriteThreadTime> threadTime = new ArrayList<WriteThreadTime>();

	public List<WriteThreadTime> getThreadTime() {
		return threadTime;
	}

	public void setThreadTime(List<WriteThreadTime> threadTime) {
		this.threadTime = threadTime;
	}
	
	public void add(String tableName, String threadName, int rowsUpserted, long timeInMsPerMillionRows) {
		threadTime.add(new WriteThreadTime(tableName, threadName, rowsUpserted, timeInMsPerMillionRows));	
	}
	
	public String getCsvTitle() {
		return "TABLE_NAME,THREAD_NAME,ROWS_UPSERTED,TIME_IN_MS_PER_" + PherfConstants.LOG_PER_NROWS + "_ROWS\n";
	}
}

class WriteThreadTime {
	private String tableName;
	private String threadName;
	private int rowsUpserted;
	private long timeInMsPerMillionRows;
	
	public WriteThreadTime(String tableName, String threadName, int rowsUpserted, long timeInMsPerMillionRows) {
		this.tableName = tableName;
		this.threadName = threadName;
		this.rowsUpserted = rowsUpserted;
		this.timeInMsPerMillionRows = timeInMsPerMillionRows;
	}
	
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getThreadName() {
		return threadName;
	}
	public void setThreadName(String threadName) {
		this.threadName = threadName;
	}
	public long getTimeInMsPerMillionRows() {
		return timeInMsPerMillionRows;
	}
	public void setTimeInMsPerMillionRows(long timeInMsPerMillionRows) {
		this.timeInMsPerMillionRows = timeInMsPerMillionRows;
	}
	
	public List<ResultValue> getCsvRepresentation(ResultUtil util) {
        List<ResultValue> rowValues = new ArrayList<>();
        rowValues.add(new ResultValue(util.convertNull(getTableName())));
        rowValues.add(new ResultValue(util.convertNull(getThreadName())));
        rowValues.add(new ResultValue(util.convertNull(String.valueOf(getRowsUpserted()))));
        rowValues.add(new ResultValue(util.convertNull(String.valueOf(getTimeInMsPerMillionRows()))));

        return rowValues;
	}

	public int getRowsUpserted() {
		return rowsUpserted;
	}

	public void setRowsUpserted(int rowsUpserted) {
		this.rowsUpserted = rowsUpserted;
	}
}