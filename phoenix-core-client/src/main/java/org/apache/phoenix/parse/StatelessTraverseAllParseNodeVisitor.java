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

package org.apache.phoenix.parse;

import java.sql.SQLException;
import java.util.List;

public class StatelessTraverseAllParseNodeVisitor extends TraverseAllParseNodeVisitor<Void> {
    @Override
    protected void enterParseNode(ParseNode node) {
    }
    
    @Override
    public Void visitLeave(LikeParseNode node, List<Void> l) throws SQLException {
        return null;
    }

    @Override
    public Void visitLeave(AndParseNode node, List<Void> l) throws SQLException {
        return null;
    }

    @Override
    public Void visitLeave(OrParseNode node, List<Void> l) throws SQLException {
        return null;
    }

    @Override
    public Void visitLeave(FunctionParseNode node, List<Void> l) throws SQLException {
        return null;
    }

    @Override
    public Void visitLeave(ComparisonParseNode node, List<Void> l) throws SQLException {
        return null;
    }

    @Override
    public Void visitLeave(CaseParseNode node, List<Void> l) throws SQLException {
        return null;
    }

    @Override
    public Void visitLeave(AddParseNode node, List<Void> l) throws SQLException {
        return null;
    }

    @Override
    public Void visitLeave(MultiplyParseNode node, List<Void> l) throws SQLException {
        return null;
    }

    @Override
    public Void visitLeave(DivideParseNode node, List<Void> l) throws SQLException {
        return null;
    }

    @Override
    public Void visitLeave(ModulusParseNode node, List<Void> l) throws SQLException {
        return null;
    }

    @Override
    public Void visitLeave(SubtractParseNode node, List<Void> l) throws SQLException {
        return null;
    }

    @Override
    public Void visitLeave(NotParseNode node, List<Void> l) throws SQLException {
        return null;
    }

    @Override
    public Void visitLeave(ExistsParseNode node, List<Void> l) throws SQLException {
        return null;
    }
    
    @Override
    public Void visitLeave(CastParseNode node, List<Void> l) throws SQLException {
        return null;
    }
    
    @Override
    public Void visitLeave(InListParseNode node, List<Void> l) throws SQLException {
        return null;
    }
    
    @Override
    public Void visitLeave(InParseNode node, List<Void> l) throws SQLException {
        return null;
    }

    @Override
    public Void visitLeave(IsNullParseNode node, List<Void> l)
            throws SQLException {
        return null;
    }

    @Override
    public Void visitLeave(StringConcatParseNode node, List<Void> l) throws SQLException {
        return null;
    }

    @Override
    public Void visitLeave(BetweenParseNode node, List<Void> l) throws SQLException {
        return null;
    }

    @Override
    public Void visitLeave(RowValueConstructorParseNode node, List<Void> l) throws SQLException {
        return null;
    }

    @Override
    public Void visitLeave(ArrayConstructorNode node, List<Void> l) throws SQLException {
        return null;
    }

    @Override
    public Void visitLeave(ArrayAllComparisonNode node, List<Void> l)
            throws SQLException {
        return null;
    }

    @Override
    public Void visitLeave(ArrayAnyComparisonNode node, List<Void> l)
            throws SQLException {
        return null;
    }

    @Override
    public Void visitLeave(ArrayElemRefNode node, List<Void> l)
            throws SQLException {
        return null;
    }
}
