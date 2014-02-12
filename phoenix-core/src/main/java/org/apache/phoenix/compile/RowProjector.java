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
package org.apache.phoenix.compile;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.Maps;


/**
 * 
 * Class that manages a set of projected columns accessed through the zero-based
 * column index for a SELECT clause projection. The column index may be looked up
 * via the name using {@link #getColumnIndex(String)}.
 *
 * 
 * @since 0.1
 */
public class RowProjector {
    public static final RowProjector EMPTY_PROJECTOR = new RowProjector(Collections.<ColumnProjector>emptyList(),0, true);

    private final List<? extends ColumnProjector> columnProjectors;
    private final Map<String,Integer> reverseIndex;
    private final boolean allCaseSensitive;
    private final boolean someCaseSensitive;
    private final int estimatedSize;
    private final boolean isProjectEmptyKeyValue;
    
    public RowProjector(RowProjector projector, boolean isProjectEmptyKeyValue) {
        this(projector.getColumnProjectors(), projector.getEstimatedRowByteSize(), isProjectEmptyKeyValue);
    }
    /**
     * Construct RowProjector based on a list of ColumnProjectors.
     * @param columnProjectors ordered list of ColumnProjectors corresponding to projected columns in SELECT clause
     * aggregating coprocessor. Only required in the case of an aggregate query with a limit clause and otherwise may
     * be null.
     * @param estimatedRowSize 
     */
    public RowProjector(List<? extends ColumnProjector> columnProjectors, int estimatedRowSize, boolean isProjectEmptyKeyValue) {
        this.columnProjectors = Collections.unmodifiableList(columnProjectors);
        int position = columnProjectors.size();
        reverseIndex = Maps.newHashMapWithExpectedSize(position);
        boolean allCaseSensitive = true;
        boolean someCaseSensitive = false;
        for (--position; position >= 0; position--) {
            ColumnProjector colProjector = columnProjectors.get(position);
            allCaseSensitive &= colProjector.isCaseSensitive();
            someCaseSensitive |= colProjector.isCaseSensitive();
            reverseIndex.put(colProjector.getName(), position);
        }
        this.allCaseSensitive = allCaseSensitive;
        this.someCaseSensitive = someCaseSensitive;
        this.estimatedSize = estimatedRowSize;
        this.isProjectEmptyKeyValue = isProjectEmptyKeyValue;
    }
    
    public boolean isProjectEmptyKeyValue() {
        return isProjectEmptyKeyValue;
    }
    
    public List<? extends ColumnProjector> getColumnProjectors() {
        return columnProjectors;
    }
    
    public int getColumnIndex(String name) throws SQLException {
        if (!someCaseSensitive) {
            name = SchemaUtil.normalizeIdentifier(name);
        }
        Integer index = reverseIndex.get(name);
        if (index == null) {
            if (!allCaseSensitive && someCaseSensitive) {
                name = SchemaUtil.normalizeIdentifier(name);
                index = reverseIndex.get(name);
            }
            if (index == null) {
                throw new ColumnNotFoundException(name);
            }
        }
        return index;
    }
    
    public ColumnProjector getColumnProjector(int index) {
        return columnProjectors.get(index);
    }
 
    public int getColumnCount() {
        return columnProjectors.size();
    }
    
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder("[");
        for (ColumnProjector projector : columnProjectors) {
            buf.append(projector.getExpression());
            buf.append(',');
        }
        if (buf.length() > 1) {
            buf.setLength(buf.length()-1);
        }
        buf.append(']');
        return buf.toString();
    }

    public int getEstimatedRowByteSize() {
        return estimatedSize;
    }
}