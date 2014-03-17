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

import java.lang.reflect.Constructor;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.exception.UnknownFunctionException;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.expression.function.AvgAggregateFunction;
import org.apache.phoenix.expression.function.CountAggregateFunction;
import org.apache.phoenix.expression.function.CurrentDateFunction;
import org.apache.phoenix.expression.function.CurrentTimeFunction;
import org.apache.phoenix.expression.function.DistinctCountAggregateFunction;
import org.apache.phoenix.expression.function.FunctionExpression;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunctionInfo;
import org.apache.phoenix.parse.JoinTableNode.JoinType;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;


/**
 *
 * Factory used by parser to construct object model while parsing a SQL statement
 *
 * 
 * @since 0.1
 */
public class ParseNodeFactory {
    private static final String ARRAY_ELEM = "ARRAY_ELEM";
	// TODO: Use Google's Reflection library instead to find aggregate functions
    @SuppressWarnings("unchecked")
    private static final List<Class<? extends FunctionExpression>> CLIENT_SIDE_BUILT_IN_FUNCTIONS = Arrays.<Class<? extends FunctionExpression>>asList(
        CurrentDateFunction.class,
        CurrentTimeFunction.class,
        AvgAggregateFunction.class
        );
    private static final Map<BuiltInFunctionKey, BuiltInFunctionInfo> BUILT_IN_FUNCTION_MAP = Maps.newHashMap();

    /**
     *
     * Key used to look up a built-in function using the combination of
     * the lowercase name and the number of arguments. This disambiguates
     * the aggregate MAX(<col>) from the non aggregate MAX(<col1>,<col2>).
     *
     * 
     * @since 0.1
     */
    private static class BuiltInFunctionKey {
        private final String upperName;
        private final int argCount;

        private BuiltInFunctionKey(String lowerName, int argCount) {
            this.upperName = lowerName;
            this.argCount = argCount;
        }
        
        @Override
        public String toString() {
            return upperName;
        }
        
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + argCount;
            result = prime * result + ((upperName == null) ? 0 : upperName.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            BuiltInFunctionKey other = (BuiltInFunctionKey)obj;
            if (argCount != other.argCount) return false;
            if (!upperName.equals(other.upperName)) return false;
            return true;
        }
    }

    private static void addBuiltInFunction(Class<? extends FunctionExpression> f) throws Exception {
        BuiltInFunction d = f.getAnnotation(BuiltInFunction.class);
        if (d == null) {
            return;
        }
        int nArgs = d.args().length;
        BuiltInFunctionInfo value = new BuiltInFunctionInfo(f, d);
        do {
            // Add function to function map, throwing if conflicts found
            // Add entry for each possible version of function based on arguments that are not required to be present (i.e. arg with default value)
            BuiltInFunctionKey key = new BuiltInFunctionKey(value.getName(), nArgs);
            if (BUILT_IN_FUNCTION_MAP.put(key, value) != null) {
                throw new IllegalStateException("Multiple " + value.getName() + " functions with " + nArgs + " arguments");
            }
        } while (--nArgs >= 0 && d.args()[nArgs].defaultValue().length() > 0);

        // Look for default values that aren't at the end and throw
        while (--nArgs >= 0) {
            if (d.args()[nArgs].defaultValue().length() > 0) {
                throw new IllegalStateException("Function " + value.getName() + " has non trailing default value of '" + d.args()[nArgs].defaultValue() + "'. Only trailing arguments may have default values");
            }
        }
    }
    /**
     * Reflect this class and populate static structures from it.
     * Don't initialize in static block because we have a circular dependency
     */
    private synchronized static void initBuiltInFunctionMap() {
        if (!BUILT_IN_FUNCTION_MAP.isEmpty()) {
            return;
        }
        Class<? extends FunctionExpression> f = null;
        try {
            // Reflection based parsing which yields direct explicit function evaluation at runtime
            for (int i = 0; i < CLIENT_SIDE_BUILT_IN_FUNCTIONS.size(); i++) {
                f = CLIENT_SIDE_BUILT_IN_FUNCTIONS.get(i);
                addBuiltInFunction(f);
            }
            for (ExpressionType et : ExpressionType.values()) {
                Class<? extends Expression> ec = et.getExpressionClass();
                if (FunctionExpression.class.isAssignableFrom(ec)) {
                    @SuppressWarnings("unchecked")
                    Class<? extends FunctionExpression> c = (Class<? extends FunctionExpression>)ec;
                    addBuiltInFunction(f = c);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed initialization of built-in functions at class '" + f + "'", e);
        }
    }

    private static BuiltInFunctionInfo getInfo(String name, List<ParseNode> children) {
        return get(SchemaUtil.normalizeIdentifier(name), children);
    }

    public static BuiltInFunctionInfo get(String normalizedName, List<ParseNode> children) {
        initBuiltInFunctionMap();
        BuiltInFunctionInfo info = BUILT_IN_FUNCTION_MAP.get(new BuiltInFunctionKey(normalizedName,children.size()));
        if (info == null) {
            throw new UnknownFunctionException(normalizedName);
        }
        return info;
    }

    public ParseNodeFactory() {
    }

    public ExplainStatement explain(BindableStatement statement) {
        return new ExplainStatement(statement);
    }

    public AliasedNode aliasedNode(String alias, ParseNode expression) {
    	return new AliasedNode(alias, expression);
    }
    
    public AddParseNode add(List<ParseNode> children) {
        return new AddParseNode(children);
    }

    public SubtractParseNode subtract(List<ParseNode> children) {
        return new SubtractParseNode(children);
    }

    public MultiplyParseNode multiply(List<ParseNode> children) {
        return new MultiplyParseNode(children);
    }

    public AndParseNode and(List<ParseNode> children) {
        return new AndParseNode(children);
    }
    
    public FamilyWildcardParseNode family(String familyName){
    	    return new FamilyWildcardParseNode(familyName, false);
    }
    
    public TableWildcardParseNode tableWildcard(TableName tableName) {
        return new TableWildcardParseNode(tableName, false);
    }

    public WildcardParseNode wildcard() {
        return WildcardParseNode.INSTANCE;
    }

    public BetweenParseNode between(ParseNode l, ParseNode r1, ParseNode r2, boolean negate) {
        return new BetweenParseNode(l, r1, r2, negate);
    }

    public BindParseNode bind(String bind) {
        return new BindParseNode(bind);
    }

    public StringConcatParseNode concat(List<ParseNode> children) {
        return new StringConcatParseNode(children);
    }

    public ColumnParseNode column(TableName tableName, String name, String alias) {
        return new ColumnParseNode(tableName,name,alias);
    }
    
    public ColumnName columnName(String columnName) {
        return new ColumnName(columnName);
    }

    public ColumnName columnName(String familyName, String columnName) {
        return new ColumnName(familyName, columnName);
    }

    public PropertyName propertyName(String propertyName) {
        return new PropertyName(propertyName);
    }

    public PropertyName propertyName(String familyName, String propertyName) {
        return new PropertyName(familyName, propertyName);
    }

    public ColumnDef columnDef(ColumnName columnDefName, String sqlTypeName, boolean isNull, Integer maxLength, Integer scale, boolean isPK, SortOrder sortOrder) {
        return new ColumnDef(columnDefName, sqlTypeName, isNull, maxLength, scale, isPK, sortOrder);
    }
    
    public ColumnDef columnDef(ColumnName columnDefName, String sqlTypeName, boolean isArray, Integer arrSize, Boolean isNull, Integer maxLength, Integer scale, boolean isPK, 
        	SortOrder sortOrder) {
        return new ColumnDef(columnDefName, sqlTypeName, isArray, arrSize, isNull, maxLength, scale, isPK, sortOrder);
    }

    public PrimaryKeyConstraint primaryKey(String name, List<Pair<ColumnName, SortOrder>> columnNameAndSortOrder) {
        return new PrimaryKeyConstraint(name, columnNameAndSortOrder);
    }
    
    public CreateTableStatement createTable(TableName tableName, ListMultimap<String,Pair<String,Object>> props, List<ColumnDef> columns, PrimaryKeyConstraint pkConstraint, List<ParseNode> splits, PTableType tableType, boolean ifNotExists, TableName baseTableName, ParseNode tableTypeIdNode, int bindCount) {
        return new CreateTableStatement(tableName, props, columns, pkConstraint, splits, tableType, ifNotExists, baseTableName, tableTypeIdNode, bindCount);
    }
    
    public CreateIndexStatement createIndex(NamedNode indexName, NamedTableNode dataTable, PrimaryKeyConstraint pkConstraint, List<ColumnName> includeColumns, List<ParseNode> splits, ListMultimap<String,Pair<String,Object>> props, boolean ifNotExists, int bindCount) {
        return new CreateIndexStatement(indexName, dataTable, pkConstraint, includeColumns, splits, props, ifNotExists, bindCount);
    }
    
    public CreateSequenceStatement createSequence(TableName tableName, ParseNode startsWith, ParseNode incrementBy, ParseNode cacheSize, boolean ifNotExits, int bindCount){
    	return new CreateSequenceStatement(tableName, startsWith, incrementBy, cacheSize, ifNotExits, bindCount);
    } 
    
    public DropSequenceStatement dropSequence(TableName tableName, boolean ifExits, int bindCount){
        return new DropSequenceStatement(tableName, ifExits, bindCount);
    }
    
	public SequenceValueParseNode currentValueFor(TableName tableName) {
		return new SequenceValueParseNode(tableName, SequenceValueParseNode.Op.CURRENT_VALUE);
	}
    
    public SequenceValueParseNode nextValueFor(TableName tableName) {
        return new SequenceValueParseNode(tableName, SequenceValueParseNode.Op.NEXT_VALUE);
    }
    
    public AddColumnStatement addColumn(NamedTableNode table,  PTableType tableType, List<ColumnDef> columnDefs, boolean ifNotExists, Map<String,Object> props) {
        return new AddColumnStatement(table, tableType, columnDefs, ifNotExists, props);
    }
    
    public DropColumnStatement dropColumn(NamedTableNode table,  PTableType tableType, List<ColumnName> columnNodes, boolean ifExists) {
        return new DropColumnStatement(table, tableType, columnNodes, ifExists);
    }
    
    public DropTableStatement dropTable(TableName tableName, PTableType tableType, boolean ifExists) {
        return new DropTableStatement(tableName, tableType, ifExists);
    }
    
    public DropIndexStatement dropIndex(NamedNode indexName, TableName tableName, boolean ifExists) {
        return new DropIndexStatement(indexName, tableName, ifExists);
    }
    
    public AlterIndexStatement alterIndex(NamedTableNode indexTableNode, String dataTableName, boolean ifExists, PIndexState state) {
        return new AlterIndexStatement(indexTableNode, dataTableName, ifExists, state);
    }
    
    public TableName table(String schemaName, String tableName) {
        return TableName.createNormalized(schemaName,tableName);
    }

    public NamedNode indexName(String name) {
        return new NamedNode(name);
    }

    public NamedTableNode namedTable(String alias, TableName name) {
        return new NamedTableNode(alias, name);
    }

    public NamedTableNode namedTable(String alias, TableName name ,List<ColumnDef> dyn_columns) {
        return new NamedTableNode(alias, name,dyn_columns);
    }

    public BindTableNode bindTable(String alias, TableName name) {
        return new BindTableNode(alias, name);
    }

    public CaseParseNode caseWhen(List<ParseNode> children) {
        return new CaseParseNode(children);
    }

    public DivideParseNode divide(List<ParseNode> children) {
        return new DivideParseNode(children);
    }


    public FunctionParseNode functionDistinct(String name, List<ParseNode> args) {
        if (CountAggregateFunction.NAME.equals(SchemaUtil.normalizeIdentifier(name))) {
            BuiltInFunctionInfo info = getInfo(
                    SchemaUtil.normalizeIdentifier(DistinctCountAggregateFunction.NAME), args);
            return new DistinctCountParseNode(name, args, info);
        } else {
            throw new UnsupportedOperationException("DISTINCT not supported with " + name);
        }
    }
    
    public FunctionParseNode arrayElemRef(List<ParseNode> args) {
    	return function(ARRAY_ELEM, args);
    }

    public FunctionParseNode function(String name, List<ParseNode> args) {
        BuiltInFunctionInfo info = getInfo(name, args);
        Constructor<? extends FunctionParseNode> ctor = info.getNodeCtor();
        if (ctor == null) {
            return info.isAggregate()
            ? new AggregateFunctionParseNode(name, args, info)
            : new FunctionParseNode(name, args, info);
        } else {
            try {
                return ctor.newInstance(name, args, info);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public FunctionParseNode function(String name, List<ParseNode> valueNodes,
            List<ParseNode> columnNodes, boolean isAscending) {
        // Right now we support PERCENT functions on only one column
        if (valueNodes.size() != 1 || columnNodes.size() != 1) {
            throw new UnsupportedOperationException(name + " not supported on multiple columns");
        }
        List<ParseNode> children = new ArrayList<ParseNode>(3);
        children.add(columnNodes.get(0));
        children.add(new LiteralParseNode(Boolean.valueOf(isAscending)));
        children.add(valueNodes.get(0));
        return function(name, children);
    }
    

    public HintNode hint(String hint) {
        return new HintNode(hint);
    }

    public InListParseNode inList(List<ParseNode> children, boolean negate) {
        return new InListParseNode(children, negate);
    }

    public ExistsParseNode exists(ParseNode l, ParseNode r, boolean negate) {
        return new ExistsParseNode(l, r, negate);
    }

    public InParseNode in(ParseNode l, ParseNode r, boolean negate) {
        return new InParseNode(l, r, negate);
    }

    public IsNullParseNode isNull(ParseNode child, boolean negate) {
        return new IsNullParseNode(child, negate);
    }

    public JoinTableNode join (JoinType type, ParseNode on, TableNode table) {
        return new JoinTableNode(type, on, table);
    }

    public DerivedTableNode derivedTable (String alias, SelectStatement select) {
        return new DerivedTableNode(alias, select);
    }

    public LikeParseNode like(ParseNode lhs, ParseNode rhs, boolean negate) {
        return new LikeParseNode(lhs, rhs, negate);
    }


    public LiteralParseNode literal(Object value) {
        return new LiteralParseNode(value);
    }

    public CastParseNode cast(ParseNode expression, String dataType, Integer maxLength, Integer scale) {
        return new CastParseNode(expression, dataType, maxLength, scale, false);
    }

    public CastParseNode cast(ParseNode expression, PDataType dataType, Integer maxLength, Integer scale) {
        return new CastParseNode(expression, dataType, maxLength, scale, false);
    }

    public CastParseNode cast(ParseNode expression, PDataType dataType, Integer maxLength, Integer scale, boolean arr) {
        return new CastParseNode(expression, dataType, maxLength, scale, arr);
    }

    public CastParseNode cast(ParseNode expression, String dataType, Integer maxLength, Integer scale, boolean arr) {
        return new CastParseNode(expression, dataType, maxLength, scale, arr);
    }
    
    public ParseNode rowValueConstructor(List<ParseNode> l) {
        return new RowValueConstructorParseNode(l);
    }
    
    private void checkTypeMatch (PDataType expectedType, PDataType actualType) throws SQLException {
        if (!expectedType.isCoercibleTo(actualType)) {
            throw TypeMismatchException.newException(expectedType, actualType);
        }
    }

    public LiteralParseNode literal(Object value, PDataType expectedType) throws SQLException {
        PDataType actualType = PDataType.fromLiteral(value);
        if (actualType != null && actualType != expectedType) {
            checkTypeMatch(expectedType, actualType);
            value = expectedType.toObject(value, actualType);
        }
        return new LiteralParseNode(value);
    }

    public LiteralParseNode coerce(LiteralParseNode literalNode, PDataType expectedType) throws SQLException {
        PDataType actualType = literalNode.getType();
        if (actualType != null) {
            Object before = literalNode.getValue();
            checkTypeMatch(expectedType, actualType);
            Object after = expectedType.toObject(before, actualType);
            if (before != after) {
                literalNode = literal(after);
            }
        }
        return literalNode;
    }

    public ComparisonParseNode comparison(CompareOp op, ParseNode lhs, ParseNode rhs) {
        switch (op){
        case LESS:
            return lt(lhs,rhs);
        case LESS_OR_EQUAL:
            return lte(lhs,rhs);
        case EQUAL:
            return equal(lhs,rhs);
        case NOT_EQUAL:
            return notEqual(lhs,rhs);
        case GREATER_OR_EQUAL:
            return gte(lhs,rhs);
        case GREATER:
            return gt(lhs,rhs);
        default:
            throw new IllegalArgumentException("Unexpcted CompareOp of " + op);
        }
    }

    public GreaterThanParseNode gt(ParseNode lhs, ParseNode rhs) {
        return new GreaterThanParseNode(lhs, rhs);
    }


    public GreaterThanOrEqualParseNode gte(ParseNode lhs, ParseNode rhs) {
        return new GreaterThanOrEqualParseNode(lhs, rhs);
    }

    public LessThanParseNode lt(ParseNode lhs, ParseNode rhs) {
        return new LessThanParseNode(lhs, rhs);
    }


    public LessThanOrEqualParseNode lte(ParseNode lhs, ParseNode rhs) {
        return new LessThanOrEqualParseNode(lhs, rhs);
    }

    public EqualParseNode equal(ParseNode lhs, ParseNode rhs) {
        return new EqualParseNode(lhs, rhs);
    }

    public ArrayConstructorNode upsertStmtArrayNode(List<ParseNode> upsertStmtArray) {
    	return new ArrayConstructorNode(upsertStmtArray);
    }

    public MultiplyParseNode negate(ParseNode child) {
        return new MultiplyParseNode(Arrays.asList(child,this.literal(-1)));
    }

    public NotEqualParseNode notEqual(ParseNode lhs, ParseNode rhs) {
        return new NotEqualParseNode(lhs, rhs);
    }

    public NotParseNode not(ParseNode child) {
        return new NotParseNode(child);
    }


    public OrParseNode or(List<ParseNode> children) {
        return new OrParseNode(children);
    }


    public OrderByNode orderBy(ParseNode expression, boolean nullsLast, boolean orderAscending) {
        return new OrderByNode(expression, nullsLast, orderAscending);
    }


    public OuterJoinParseNode outer(ParseNode node) {
        return new OuterJoinParseNode(node);
    }
    
    public SelectStatement select(List<? extends TableNode> from, HintNode hint, boolean isDistinct, List<AliasedNode> select, ParseNode where,
            List<ParseNode> groupBy, ParseNode having, List<OrderByNode> orderBy, LimitNode limit, int bindCount, boolean isAggregate) {

        return new SelectStatement(from, hint, isDistinct, select, where, groupBy == null ? Collections.<ParseNode>emptyList() : groupBy, having, orderBy == null ? Collections.<OrderByNode>emptyList() : orderBy, limit, bindCount, isAggregate);
    }
    
    public UpsertStatement upsert(NamedTableNode table, HintNode hint, List<ColumnName> columns, List<ParseNode> values, SelectStatement select, int bindCount) {
        return new UpsertStatement(table, hint, columns, values, select, bindCount);
    }
    
    public DeleteStatement delete(NamedTableNode table, HintNode hint, ParseNode node, List<OrderByNode> orderBy, LimitNode limit, int bindCount) {
        return new DeleteStatement(table, hint, node, orderBy, limit, bindCount);
    }

    public SelectStatement select(SelectStatement statement, ParseNode where, ParseNode having) {
        return select(statement.getFrom(), statement.getHint(), statement.isDistinct(), statement.getSelect(), where, statement.getGroupBy(), having, statement.getOrderBy(), statement.getLimit(), statement.getBindCount(), statement.isAggregate());
    }

    public SelectStatement select(SelectStatement statement, List<? extends TableNode> tables) {
        return select(tables, statement.getHint(), statement.isDistinct(), statement.getSelect(), statement.getWhere(), statement.getGroupBy(), statement.getHaving(), statement.getOrderBy(), statement.getLimit(), statement.getBindCount(), statement.isAggregate());
    }

    public SelectStatement select(SelectStatement statement, HintNode hint) {
        return hint == null || hint.isEmpty() ? statement : select(statement.getFrom(), hint, statement.isDistinct(), statement.getSelect(), statement.getWhere(), statement.getGroupBy(), statement.getHaving(), statement.getOrderBy(), statement.getLimit(), statement.getBindCount(), statement.isAggregate());
    }

    public SubqueryParseNode subquery(SelectStatement select) {
        return new SubqueryParseNode(select);
    }

    public LimitNode limit(BindParseNode b) {
        return new LimitNode(b);
    }

    public LimitNode limit(LiteralParseNode l) {
        return new LimitNode(l);
    }
}
