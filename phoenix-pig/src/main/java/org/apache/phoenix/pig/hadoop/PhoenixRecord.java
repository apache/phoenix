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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.phoenix.pig.util.TypeUtil;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataType;

import com.google.common.base.Preconditions;

/**
 * A {@link Writable} representing a Phoenix record. This class
 * a) does a type mapping and sets the value accordingly in the {@link PreparedStatement}
 * b) reads the column values from the {@link ResultSet}
 * 
 */
public class PhoenixRecord implements Writable {
	
	private final List<Object> values;
	private final ResourceFieldSchema[] fieldSchemas;
	
	public PhoenixRecord() {
	    this(null);
	}
	
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
			ColumnInfo columnInfo = columnMetadataList.get(i);
			byte type = (fieldSchemas == null) ? DataType.findType(o) : fieldSchemas[i].getType();
			try {
                Object upsertValue = convertTypeSpecificValue(o, type, columnInfo.getSqlType());
                if (upsertValue != null) {
                    statement.setObject(i + 1, upsertValue, columnInfo.getSqlType());
                } else {
                    statement.setNull(i + 1, columnInfo.getSqlType());
                }
            } catch (RuntimeException re) {
                throw new RuntimeException(String.format("Unable to process column %s, innerMessage=%s"
                        ,columnInfo.toString(),re.getMessage()),re);
                
            }
		}
		
		statement.execute();
	}
	
	public void read(final ResultSet rs, final int noOfColumns) throws SQLException {
	    Preconditions.checkNotNull(rs);
        Preconditions.checkArgument(noOfColumns > 0, "No of arguments passed is <= 0");
        values.clear();
        for(int i = 1 ; i <= noOfColumns ; i++) {
            Object obj = rs.getObject(i);
            values.add(obj);
        }
	}
	
	public void add(Object value) {
		values.add(value);
	}

	private Object convertTypeSpecificValue(Object o, byte type, Integer sqlType) {
		PDataType pDataType = PDataType.fromTypeId(sqlType);

		return TypeUtil.castPigTypeToPhoenix(o, type, pDataType);
	}

    public List<Object> getValues() {
        return values;
    }
}
