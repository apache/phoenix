/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.pig.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataType;

import org.apache.phoenix.pig.TypeUtil;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.util.ColumnInfo;

/**
 * A {@link Writable} representing a Phoenix record. This class
 * does a type mapping and sets the value accordingly in the 
 * {@link PreparedStatement}
 * 
 * 
 *
 */
public class PhoenixRecord implements Writable {
	
	private final List<Object> values;
	private final ResourceFieldSchema[] fieldSchemas;
	
	public PhoenixRecord(ResourceFieldSchema[] fieldSchemas) {
		this.values = new ArrayList<Object>();
		this.fieldSchemas = fieldSchemas;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {		
	}

	@Override
	public void write(DataOutput out) throws IOException {		
	}
	
	public void write(PreparedStatement statement, List<ColumnInfo> columnMetadataList) throws SQLException {
		for (int i = 0; i < columnMetadataList.size(); i++) {
			Object o = values.get(i);
			
			byte type = (fieldSchemas == null) ? DataType.findType(o) : fieldSchemas[i].getType();
			Object upsertValue = convertTypeSpecificValue(o, type, columnMetadataList.get(i).getSqlType());

			if (upsertValue != null) {
				statement.setObject(i + 1, upsertValue, columnMetadataList.get(i).getSqlType());
			} else {
				statement.setNull(i + 1, columnMetadataList.get(i).getSqlType());
			}
		}
		
		statement.execute();
	}
	
	public void add(Object value) {
		values.add(value);
	}

	private Object convertTypeSpecificValue(Object o, byte type, Integer sqlType) {
		PDataType pDataType = PDataType.fromTypeId(sqlType);

		return TypeUtil.castPigTypeToPhoenix(o, type, pDataType);
	}
}
