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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TrustedByteArrayOutputStream;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;


/**
 * 
 * Class that manages a set of projected columns accessed through the zero-based
 * column index for a SELECT clause projection. The column index may be looked up
 * via the name using {@link #getColumnIndex(String)}.
 *
 * 
 * @since 0.1
 */
public class RowProjector implements Cloneable {
    public static final RowProjector EMPTY_PROJECTOR = new RowProjector(Collections.<ColumnProjector>emptyList(),0, true);

    private final List<? extends ColumnProjector> columnProjectors;
    private final ListMultimap<String,Integer> reverseIndex;
    private final boolean allCaseSensitive;
    private final boolean someCaseSensitive;
    private final int estimatedSize;
    private final boolean isProjectEmptyKeyValue;
    private volatile List<byte[]> rowExpressions;
    private List<Expression> statelessExpressions;
    
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
        reverseIndex = ArrayListMultimap.<String, Integer>create();
        boolean allCaseSensitive = true;
        boolean someCaseSensitive = false;
        for (--position; position >= 0; position--) {
            ColumnProjector colProjector = columnProjectors.get(position);
            allCaseSensitive &= colProjector.isCaseSensitive();
            someCaseSensitive |= colProjector.isCaseSensitive();
            reverseIndex.put(colProjector.getName(), position);
            if (!colProjector.getTableName().isEmpty()) {
                reverseIndex.put(SchemaUtil.getColumnName(colProjector.getTableName(), colProjector.getName()), position);
            }
        }
        this.allCaseSensitive = allCaseSensitive;
        this.someCaseSensitive = someCaseSensitive;
        this.estimatedSize = estimatedRowSize;
        this.isProjectEmptyKeyValue = isProjectEmptyKeyValue;
    }

    @Override
    public RowProjector clone() {
        return cloneRowProjector();
    }

    private RowProjector cloneRowProjector() {
        if (rowExpressions == null) {
            synchronized(this) {
                if (rowExpressions == null) {
                    List<byte[]> localRowExpressions = new ArrayList<byte[]>(this.getColumnCount());
                    try {
                        for (int i = 0; i < this.getColumnCount(); i++) {
                             TrustedByteArrayOutputStream bytesOut = new TrustedByteArrayOutputStream(1024);
                             DataOutputStream output = new DataOutputStream(bytesOut);
                             Expression expression = this.getColumnProjector(i).getExpression();
                             // Hack to clone expressions by serializing them. Not all expressions
                             // are serializable. For example NEXT VALUE FOR is client-side only.
                             // In this case, we keep the original exception and restore it when
                             // we clone (as we don't need to clone it).
                             // TODO: visitor that clones based on Determinism of PER_INVOCATION
                             // instead, that way we'd only be cloning expression trees with RAND.
                             ExpressionType expressionType = ExpressionType.valueOfOrNull(expression);
                             if (expressionType == null) {
                                 if (statelessExpressions == null) {
                                     statelessExpressions = Lists.newArrayList();
                                 }
                                 statelessExpressions.add(expression);
                                 WritableUtils.writeVInt(output, -statelessExpressions.size());
                             } else {
                                 WritableUtils.writeVInt(output, expressionType.ordinal());
                                 expression.write(output);
                             }
                             output.flush();
                             localRowExpressions.add(bytesOut.getBuffer());
                        }
                    } catch (IOException io) {
                        throw new RuntimeException(io);
                    }
                    rowExpressions = localRowExpressions;
                }
            }
        }
        List<ColumnProjector> colProjectors = new ArrayList<ColumnProjector>(rowExpressions.size());
        try {
            for (int i=0; i<rowExpressions.size(); i++) {
                ByteArrayInputStream bytesIn = new ByteArrayInputStream(rowExpressions.get(i));
                DataInputStream input = new DataInputStream(bytesIn);
                int ordinal = WritableUtils.readVInt(input);
                Expression expression;
                if (ordinal < 0) {
                    expression = statelessExpressions.get(ordinal+1);
                } else {
                    expression = ExpressionType.values()[ordinal].newInstance();
                    expression.readFields(input);
                }
                ColumnProjector colProjector = this.getColumnProjector(i);
                colProjectors.add(new ExpressionProjector(colProjector.getName(),
                    colProjector.getTableName(), 
                    expression,
                    colProjector.isCaseSensitive()));
            }
            return new RowProjector(colProjectors, 
                this.getEstimatedRowByteSize(),
                this.isProjectEmptyKeyValue());
        } catch (IOException io) {
            throw new RuntimeException(io);
        }
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
        List<Integer> index = reverseIndex.get(name);
        if (index.isEmpty()) {
            if (!allCaseSensitive && someCaseSensitive) {
                name = SchemaUtil.normalizeIdentifier(name);
                index = reverseIndex.get(name);
            }
            if (index.isEmpty()) {
                throw new ColumnNotFoundException(name);
            }
        }
        if (index.size() > 1) {
            throw new AmbiguousColumnException(name);
        }
        
        return index.get(0);
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

    /**
     * allow individual expressions to reset their state between rows
     */
    public void reset() {
        for (ColumnProjector projector : columnProjectors) {
            projector.getExpression().reset();
        }
    }
}
