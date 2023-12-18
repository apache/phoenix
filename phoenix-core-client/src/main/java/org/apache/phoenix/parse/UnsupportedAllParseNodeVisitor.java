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
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;


/**
 * 
 * Visitor that throws UnsupportedOperationException for every
 * node.  Meant to be sub-classed for the case of a small subset
 * of nodes being supported, in which case only those applicable
 * methods would be overridden.
 *
 * 
 * @since 0.1
 */
abstract public class UnsupportedAllParseNodeVisitor<E> extends BaseParseNodeVisitor<E> {

    @Override
    public E visit(ColumnParseNode node) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }

    @Override
    public E visit(LiteralParseNode node) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }

	@Override
	public boolean visitEnter(ArrayConstructorNode node) throws SQLException {
		throw new SQLFeatureNotSupportedException(node.toString());
	}

	@Override
	public E visitLeave(ArrayConstructorNode node, List<E> l)
			throws SQLException {
		throw new SQLFeatureNotSupportedException(node.toString());
	}

    @Override
    public E visit(BindParseNode node) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }

    @Override
    public E visit(WildcardParseNode node) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }

    @Override
    public E visit(TableWildcardParseNode node) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }

    @Override
    public E visit(FamilyWildcardParseNode node) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }

    @Override
    public E visit(SubqueryParseNode node) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }

    @Override
    public boolean visitEnter(AndParseNode node) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }

    @Override
    public boolean visitEnter(OrParseNode node) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }

    @Override
    public boolean visitEnter(FunctionParseNode node) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }

    @Override
    public boolean visitEnter(ComparisonParseNode node) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }
    
    @Override
    public boolean visitEnter(BetweenParseNode node) throws SQLException{
        throw new SQLFeatureNotSupportedException(node.toString());
    }
    
    @Override
    public E visitLeave(AndParseNode node, List<E> l) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }
    
    @Override
    public E visitLeave(OrParseNode node, List<E> l) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }

    @Override
    public E visitLeave(FunctionParseNode node, List<E> l) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }

    @Override
    public E visitLeave(ComparisonParseNode node, List<E> l) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }

    @Override
    public boolean visitEnter(LikeParseNode node) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }

    @Override
    public E visitLeave(LikeParseNode node, List<E> l) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }

    @Override
    public E visitLeave(NotParseNode node, List<E> l) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }
    
    @Override
    public boolean visitEnter(NotParseNode node) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }

    @Override
    public E visitLeave(ExistsParseNode node, List<E> l) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }
    
    @Override
    public boolean visitEnter(ExistsParseNode node) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }
    
    @Override
    public E visitLeave(CastParseNode node, List<E> l) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }
    
    @Override
    public boolean visitEnter(CastParseNode node) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }
    
    @Override
    public E visitLeave(InListParseNode node, List<E> l) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }
    
    @Override
    public E visitLeave(InParseNode node, List<E> l) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }

    @Override
    public E visitLeave(BetweenParseNode node, List<E> l) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }
    
    @Override
    public boolean visitEnter(InListParseNode node) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }
    
    @Override
    public boolean visitEnter(InParseNode node) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }

    @Override
    public boolean visitEnter(IsNullParseNode node) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }

    @Override
    public E visitLeave(IsNullParseNode node, List<E> l) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }

    @Override
    public boolean visitEnter(AddParseNode node) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }

    @Override
    public E visitLeave(AddParseNode node, List<E> l) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }

    @Override
    public boolean visitEnter(SubtractParseNode node) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }

    @Override
    public E visitLeave(SubtractParseNode node, List<E> l) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }

    @Override
    public boolean visitEnter(MultiplyParseNode node) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }

    @Override
    public E visitLeave(MultiplyParseNode node, List<E> l) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }

    @Override
    public boolean visitEnter(ModulusParseNode node) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }

    @Override
    public E visitLeave(ModulusParseNode node, List<E> l) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }

    @Override
    public boolean visitEnter(DivideParseNode node) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }

    @Override
    public E visitLeave(DivideParseNode node, List<E> l) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }

    @Override
    public List<E> newElementList(int size) {
        return null;
    }

    @Override
    public void addElement(List<E> a, E element) {
    }
    
    @Override
    public E visit(SequenceValueParseNode node) throws SQLException {			
		return null;
	}

    @Override
    public boolean visitEnter(ArrayAnyComparisonNode node) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }

    @Override
    public E visitLeave(ArrayAnyComparisonNode node, List<E> l) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }
    
    @Override
    public boolean visitEnter(ArrayAllComparisonNode node) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }

    @Override
    public E visitLeave(ArrayAllComparisonNode node, List<E> l) throws SQLException {
        throw new SQLFeatureNotSupportedException(node.toString());
    }
}
