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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.BindParseNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.util.ByteUtil;

import java.sql.SQLException;
import java.util.List;

public class SplitKeyUtil {

    private static final PDatum VARBINARY_DATUM = new VarbinaryDatum();

    private static class VarbinaryDatum implements PDatum {

        @Override
        public boolean isNullable() {
            return false;
        }

        @Override
        public PDataType getDataType() {
            return PVarbinary.INSTANCE;
        }

        @Override
        public Integer getMaxLength() {
            return null;
        }

        @Override
        public Integer getScale() {
            return null;
        }

        @Override
        public SortOrder getSortOrder() {
            return SortOrder.getDefault();
        }

    }

    static byte[][] getSplits(List<ParseNode> splitNodes, StatementContext context) throws SQLException {
        final byte[][] splits = new byte[splitNodes.size()][];
        ImmutableBytesWritable ptr = context.getTempPtr();
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(context);
        for (int i = 0; i < splits.length; i++) {
            ParseNode node = splitNodes.get(i);
            if (node instanceof BindParseNode) {
                context.getBindManager().addParamMetaData((BindParseNode) node, VARBINARY_DATUM);
            }
            if (node.isStateless()) {
                Expression expression = node.accept(expressionCompiler);
                if (expression.evaluate(null, ptr)) {;
                    splits[i] = ByteUtil.copyKeyBytesIfNecessary(ptr);
                    continue;
                }
            }
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.SPLIT_POINT_NOT_CONSTANT)
                    .setMessage("Node: " + node).build().buildException();
        }
        return splits;
    }
}
