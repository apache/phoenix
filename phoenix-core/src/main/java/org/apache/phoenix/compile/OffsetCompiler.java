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

import org.apache.phoenix.parse.BindParseNode;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.LiteralParseNode;
import org.apache.phoenix.parse.OffsetNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.TraverseNoParseNodeVisitor;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;

public class OffsetCompiler {
    private static final ParseNodeFactory NODE_FACTORY = new ParseNodeFactory();

    public static final PDatum OFFSET_DATUM = new PDatum() {
        @Override
        public boolean isNullable() {
            return false;
        }

        @Override
        public PDataType getDataType() {
            return PInteger.INSTANCE;
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
    };

    private OffsetCompiler() {}

    public static Integer compile(StatementContext context, FilterableStatement statement) throws SQLException {
        OffsetNode offsetNode = statement.getOffset();
        if (offsetNode == null) { return null; }
        OffsetParseNodeVisitor visitor = new OffsetParseNodeVisitor(context);
        offsetNode.getOffsetParseNode().accept(visitor);
        return visitor.getOffset();
    }

    private static class OffsetParseNodeVisitor extends TraverseNoParseNodeVisitor<Void> {
        private final StatementContext context;
        private Integer offset;

        public OffsetParseNodeVisitor(StatementContext context) {
            this.context = context;
        }

        public Integer getOffset() {
            return offset;
        }

        @Override
        public Void visit(LiteralParseNode node) throws SQLException {
            Object offsetValue = node.getValue();
            if (offsetValue != null) {
                Integer offset = (Integer)OFFSET_DATUM.getDataType().toObject(offsetValue, node.getType());
                if (offset.intValue() >= 0) {
                    this.offset = offset;
                }
            }
            return null;
        }

        @Override
        public Void visit(BindParseNode node) throws SQLException {
            // This is for static evaluation in SubselectRewriter.
            if (context == null) return null;

            Object value = context.getBindManager().getBindValue(node);
            context.getBindManager().addParamMetaData(node, OFFSET_DATUM);
            // Resolve the bind value, create a LiteralParseNode, and call the
            // visit method for it.
            // In this way, we can deal with just having a literal on one side
            // of the expression.
            visit(NODE_FACTORY.literal(value, OFFSET_DATUM.getDataType()));
            return null;
        }

    }

}
