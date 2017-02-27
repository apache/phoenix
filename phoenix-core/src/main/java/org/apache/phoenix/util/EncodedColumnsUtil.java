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
package org.apache.phoenix.util;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.phoenix.schema.PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.expression.DelegateExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;
import org.apache.phoenix.schema.tuple.Tuple;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class EncodedColumnsUtil {

    public static boolean usesEncodedColumnNames(PTable table) {
        return usesEncodedColumnNames(table.getEncodingScheme());
    }
    
    public static boolean usesEncodedColumnNames(QualifierEncodingScheme encodingScheme) {
        return encodingScheme != null && encodingScheme != QualifierEncodingScheme.NON_ENCODED_QUALIFIERS;
    }
    
    public static void setColumns(PColumn column, PTable table, Scan scan) {
    	if (table.getImmutableStorageScheme() == ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS) {
            // if a table storage scheme is COLUMNS_STORED_IN_SINGLE_CELL set then all columns of a column family are stored in a single cell 
            // (with the qualifier name being same as the family name), just project the column family here
            // so that we can calculate estimatedByteSize correctly in ProjectionCompiler 
    		scan.addFamily(column.getFamilyName().getBytes());
    	}
        else {
            if (column.getColumnQualifierBytes() != null) {
                scan.addColumn(column.getFamilyName().getBytes(), column.getColumnQualifierBytes());
            }
        }
    }
    
    public static final boolean useNewValueColumnQualifier(Scan s) {
        // null check for backward compatibility
        return s.getAttribute(BaseScannerRegionObserver.USE_NEW_VALUE_COLUMN_QUALIFIER) == null ? false : true;
    }
    
    public static QualifierEncodingScheme getQualifierEncodingScheme(Scan s) {
        // null check for backward compatibility
        return s.getAttribute(BaseScannerRegionObserver.QUALIFIER_ENCODING_SCHEME) == null ? QualifierEncodingScheme.NON_ENCODED_QUALIFIERS : QualifierEncodingScheme.fromSerializedValue(s.getAttribute(BaseScannerRegionObserver.QUALIFIER_ENCODING_SCHEME)[0]);
    }
    
    public static ImmutableStorageScheme getImmutableStorageScheme(Scan s) {
        // null check for backward compatibility
        return s.getAttribute(BaseScannerRegionObserver.IMMUTABLE_STORAGE_ENCODING_SCHEME) == null ? ImmutableStorageScheme.ONE_CELL_PER_COLUMN : ImmutableStorageScheme.fromSerializedValue(s.getAttribute(BaseScannerRegionObserver.IMMUTABLE_STORAGE_ENCODING_SCHEME)[0]);
    }

    /**
     * @return pair of byte arrays. The first part of the pair is the empty key value's column qualifier, and the second
     *         part is the value to use for it.
     */
    public static Pair<byte[], byte[]> getEmptyKeyValueInfo(PTable table) {
        return usesEncodedColumnNames(table) ? new Pair<>(QueryConstants.ENCODED_EMPTY_COLUMN_BYTES,
                QueryConstants.ENCODED_EMPTY_COLUMN_VALUE_BYTES) : new Pair<>(QueryConstants.EMPTY_COLUMN_BYTES,
                QueryConstants.EMPTY_COLUMN_VALUE_BYTES);
    }

    /**
     * @return pair of byte arrays. The first part of the pair is the empty key value's column qualifier, and the second
     *         part is the value to use for it.
     */
    public static Pair<byte[], byte[]> getEmptyKeyValueInfo(boolean usesEncodedColumnNames) {
        return usesEncodedColumnNames ? new Pair<>(QueryConstants.ENCODED_EMPTY_COLUMN_BYTES,
                QueryConstants.ENCODED_EMPTY_COLUMN_VALUE_BYTES) : new Pair<>(QueryConstants.EMPTY_COLUMN_BYTES,
                QueryConstants.EMPTY_COLUMN_VALUE_BYTES);
    }
    
    /**
     * @return pair of byte arrays. The first part of the pair is the empty key value's column qualifier, and the second
     *         part is the value to use for it.
     */
    public static Pair<byte[], byte[]> getEmptyKeyValueInfo(QualifierEncodingScheme encodingScheme) {
        return usesEncodedColumnNames(encodingScheme) ? new Pair<>(QueryConstants.ENCODED_EMPTY_COLUMN_BYTES,
                QueryConstants.ENCODED_EMPTY_COLUMN_VALUE_BYTES) : new Pair<>(QueryConstants.EMPTY_COLUMN_BYTES,
                QueryConstants.EMPTY_COLUMN_VALUE_BYTES);
    }

    public static Pair<Integer, Integer> getMinMaxQualifiersFromScan(Scan scan) {
        Integer minQ = null, maxQ = null;
        byte[] minQualifier = scan.getAttribute(BaseScannerRegionObserver.MIN_QUALIFIER);
        if (minQualifier != null) {
            minQ = Bytes.toInt(minQualifier);
        }
        byte[] maxQualifier = scan.getAttribute(BaseScannerRegionObserver.MAX_QUALIFIER);
        if (maxQualifier != null) {
            maxQ = Bytes.toInt(maxQualifier);
        }
        if (minQualifier == null) {
            return null;
        }
        return new Pair<>(minQ, maxQ);
    }

    public static boolean setQualifierRanges(PTable table) {
        return table.getImmutableStorageScheme() != null
                && table.getImmutableStorageScheme() == ImmutableStorageScheme.ONE_CELL_PER_COLUMN
                && usesEncodedColumnNames(table) && !table.isTransactional()
                && !ScanUtil.hasDynamicColumns(table);
    }

    public static boolean useQualifierAsIndex(Pair<Integer, Integer> minMaxQualifiers) {
        return minMaxQualifiers != null;
    }

    public static Map<String, Pair<Integer, Integer>> getFamilyQualifierRanges(PTable table) {
        checkNotNull(table);
        QualifierEncodingScheme encodingScheme = table.getEncodingScheme();
        Preconditions.checkArgument(encodingScheme != NON_ENCODED_QUALIFIERS);
        if (table.getEncodedCQCounter() != null) {
            Map<String, Integer> values = table.getEncodedCQCounter().values();
            Map<String, Pair<Integer, Integer>> toReturn = Maps.newHashMapWithExpectedSize(values.size());
            for (Entry<String, Integer> e : values.entrySet()) {
                Integer lowerBound = QueryConstants.ENCODED_CQ_COUNTER_INITIAL_VALUE;
                Integer upperBound = e.getValue() - 1;
                if (lowerBound > upperBound) {
                    lowerBound = upperBound;
                }
                toReturn.put(e.getKey(), new Pair<>(lowerBound, upperBound));
            }
            return toReturn;
        }
        return Collections.emptyMap();
    }
    
    public static byte[] getColumnQualifierBytes(String columnName, Integer numberBasedQualifier, PTable table, boolean isPk) {
        QualifierEncodingScheme encodingScheme = table.getEncodingScheme();
        return getColumnQualifierBytes(columnName, numberBasedQualifier, encodingScheme, isPk);
    }
    
    public static byte[] getColumnQualifierBytes(String columnName, Integer numberBasedQualifier, QualifierEncodingScheme encodingScheme, boolean isPk) {
        if (isPk) {
            return null;
        }
        if (encodingScheme == null || encodingScheme == NON_ENCODED_QUALIFIERS) {
            return Bytes.toBytes(columnName);
        }
        return encodingScheme.encode(numberBasedQualifier);
    }
    
    public static Expression[] createColumnExpressionArray(int maxEncodedColumnQualifier) {
        // reserve the first position and offset maxEncodedColumnQualifier by ENCODED_CQ_COUNTER_INITIAL_VALUE (which is the minimum encoded column qualifier)
        int numElements = maxEncodedColumnQualifier - QueryConstants.ENCODED_CQ_COUNTER_INITIAL_VALUE + 2;
        Expression[] colValues = new Expression[numElements];
        Arrays.fill(colValues, new DelegateExpression(LiteralExpression.newConstant(null)) {
                   @Override
                   public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
                       return false;
                   }
               });
        // 0 is a reserved position, set it to a non-null value so that we can represent absence of a value using a negative offset
        colValues[0]=LiteralExpression.newConstant(QueryConstants.EMPTY_COLUMN_VALUE_BYTES);
        return colValues;
    }

    public static boolean isReservedColumnQualifier(int number) {
        if (number < 0) {
            throw new IllegalArgumentException("Negative column qualifier" + number + " not allowed ");
        }
        return number < QueryConstants.ENCODED_CQ_COUNTER_INITIAL_VALUE;
    }
    
    public static boolean isPossibleToUseEncodedCQFilter(QualifierEncodingScheme encodingScheme,
            ImmutableStorageScheme storageScheme) {
        return EncodedColumnsUtil.usesEncodedColumnNames(encodingScheme)
                && storageScheme == ImmutableStorageScheme.ONE_CELL_PER_COLUMN;
    }

}
