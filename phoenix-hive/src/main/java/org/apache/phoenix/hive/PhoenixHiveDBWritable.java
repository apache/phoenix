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
package org.apache.phoenix.hive;

import com.google.common.base.Preconditions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import org.apache.phoenix.schema.types.PDataType;


/**
* PhoenixHiveDBWritable
* PhoenixStorageHandler Serialized Class referenced in the SerDe 
*/

public class PhoenixHiveDBWritable implements Writable, DBWritable {
    private static final Log LOG = LogFactory.getLog(PhoenixHiveDBWritable.class);

    private final List<Object> values = new ArrayList();
    private PDataType[] PDataTypes = null;
    ResultSet rs;

    public PhoenixHiveDBWritable() {
    }

    public PhoenixHiveDBWritable(PDataType[] categories) {
        this.PDataTypes = categories;
    }

    public void readFields(ResultSet rs) throws SQLException {
        Preconditions.checkNotNull(rs);
        this.rs = rs;
    }

    public List<Object> getValues() {
        return this.values;
    }

    public Object get(String name) {
        try {
            return this.rs.getObject(name);
        } catch (SQLException se) {
            se.printStackTrace();
        }
        return null;
    }

    /**
     * adds the Hive Writable values 
     * @param value PreparedStatement
     */
    public void add(Object value) {
        this.values.add(value);
    }

    public void clear() {
        this.values.clear();
    }
    
    /**
     * Writes out the Hive writabke types to PhoenixDatatypes in a prepared statement
     * @param statement PreparedStatement
     */

    public void write(PreparedStatement statement) throws SQLException {
        for (int i = 0; i < this.values.size(); i++) {
            Object o = this.values.get(i);
            try {
                if (o != null) {
                    LOG.debug(" value " + o.toString() + " type "
                            + this.PDataTypes[i].getSqlTypeName() + " int value "
                            + this.PDataTypes[i].getSqlType());
                    statement.setObject(i + 1, PDataType
                            .fromTypeId(this.PDataTypes[i].getSqlType()).toObject(o.toString()));
                } else {
                    LOG.debug(" value NULL  type " + this.PDataTypes[i].getSqlTypeName()
                            + " int value " + this.PDataTypes[i].getSqlType());
                    statement.setNull(i + 1, this.PDataTypes[i].getSqlType());
                }
            } catch (RuntimeException re) {
                throw new RuntimeException(String.format(
                    "Unable to process column %s, innerMessage=%s",
                    new Object[] { re.getMessage() }), re);
            }
        }
    }

    public void readFields(DataInput in) throws IOException {
    }

    public void write(DataOutput arg0) throws IOException {
    }
}