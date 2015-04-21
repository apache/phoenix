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
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
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
import org.apache.phoenix.parse.LikeParseNode.LikeType;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.schema.stats.StatisticsCollectionScope;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
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
    private static final BigDecimal MAX_LONG = BigDecimal.valueOf(Long.MAX_VALUE);


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
    
    private static AtomicInteger tempAliasCounter = new AtomicInteger(0);
    
    public static String createTempAlias() {
        return "$" + tempAliasCounter.incrementAndGet();
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

    public ModulusParseNode modulus(List<ParseNode> children) {
        return new ModulusParseNode(children);
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

    public ColumnParseNode column(TableName tableName, String columnName, String alias) {
        return new ColumnParseNode(tableName, columnName, alias);
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

    public ColumnDef columnDef(ColumnName columnDefName, String sqlTypeName, boolean isNull, Integer maxLength, Integer scale, boolean isPK, SortOrder sortOrder, String expressionStr) {
        return new ColumnDef(columnDefName, sqlTypeName, isNull, maxLength, scale, isPK, sortOrder, expressionStr);
    }

    public ColumnDef columnDef(ColumnName columnDefName, String sqlTypeName, boolean isArray, Integer arrSize, Boolean isNull, Integer maxLength, Integer scale, boolean isPK, 
        	SortOrder sortOrder) {
        return new ColumnDef(columnDefName, sqlTypeName, isArray, arrSize, isNull, maxLength, scale, isPK, sortOrder, null);
    }

    public PrimaryKeyConstraint primaryKey(String name, List<Pair<ColumnName, SortOrder>> columnNameAndSortOrder) {
        return new PrimaryKeyConstraint(name, columnNameAndSortOrder);
    }
    
    public IndexKeyConstraint indexKey( List<Pair<ParseNode, SortOrder>> parseNodeAndSortOrder) {
        return new IndexKeyConstraint(parseNodeAndSortOrder);
    }

    public CreateTableStatement createTable(TableName tableName, ListMultimap<String,Pair<String,Object>> props, List<ColumnDef> columns, PrimaryKeyConstraint pkConstraint, List<ParseNode> splits, PTableType tableType, boolean ifNotExists, TableName baseTableName, ParseNode tableTypeIdNode, int bindCount) {
        return new CreateTableStatement(tableName, props, columns, pkConstraint, splits, tableType, ifNotExists, baseTableName, tableTypeIdNode, bindCount);
    }

    public CreateIndexStatement createIndex(NamedNode indexName, NamedTableNode dataTable, IndexKeyConstraint ikConstraint, List<ColumnName> includeColumns, List<ParseNode> splits, ListMultimap<String,Pair<String,Object>> props, boolean ifNotExists, IndexType indexType,boolean async, int bindCount) {
        return new CreateIndexStatement(indexName, dataTable, ikConstraint, includeColumns, splits, props, ifNotExists, indexType, async, bindCount);
    }

    public CreateSequenceStatement createSequence(TableName tableName, ParseNode startsWith,
            ParseNode incrementBy, ParseNode cacheSize, ParseNode minValue, ParseNode maxValue,
            boolean cycle, boolean ifNotExits, int bindCount) {
        return new CreateSequenceStatement(tableName, startsWith, incrementBy, cacheSize, minValue,
                maxValue, cycle, ifNotExits, bindCount);
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

    public AddColumnStatement addColumn(NamedTableNode table,  PTableType tableType, List<ColumnDef> columnDefs, boolean ifNotExists, ListMultimap<String,Pair<String,Object>> props) {
        return new AddColumnStatement(table, tableType, columnDefs, ifNotExists, props);
    }

    public DropColumnStatement dropColumn(NamedTableNode table,  PTableType tableType, List<ColumnName> columnNodes, boolean ifExists) {
        return new DropColumnStatement(table, tableType, columnNodes, ifExists);
    }

    public DropTableStatement dropTable(TableName tableName, PTableType tableType, boolean ifExists, boolean cascade) {
        return new DropTableStatement(tableName, tableType, ifExists, cascade);
    }

    public DropIndexStatement dropIndex(NamedNode indexName, TableName tableName, boolean ifExists) {
        return new DropIndexStatement(indexName, tableName, ifExists);
    }

    public AlterIndexStatement alterIndex(NamedTableNode indexTableNode, String dataTableName, boolean ifExists, PIndexState state) {
        return new AlterIndexStatement(indexTableNode, dataTableName, ifExists, state);
    }

    public TraceStatement trace(boolean isTraceOn, double samplingRate) {
        return new TraceStatement(isTraceOn, samplingRate);
    }

    public AlterSessionStatement alterSession(Map<String,Object> props) {
        return new AlterSessionStatement(props);
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

    public UpdateStatisticsStatement updateStatistics(NamedTableNode table, StatisticsCollectionScope scope, Map<String,Object> props) {
      return new UpdateStatisticsStatement(table, scope, props);
    }


    public FunctionParseNode functionDistinct(String name, List<ParseNode> args) {
        if (CountAggregateFunction.NAME.equals(SchemaUtil.normalizeIdentifier(name))) {
            BuiltInFunctionInfo info = getInfo(
                    SchemaUtil.normalizeIdentifier(DistinctCountAggregateFunction.NAME), args);
            return new DistinctCountParseNode(DistinctCountAggregateFunction.NAME, args, info);
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

        List<ParseNode> args = Lists.newArrayListWithExpectedSize(columnNodes.size() + valueNodes.size() + 1);
        args.addAll(columnNodes);
        args.add(new LiteralParseNode(Boolean.valueOf(isAscending)));
        args.addAll(valueNodes);

        BuiltInFunctionInfo info = getInfo(name, args);
        Constructor<? extends FunctionParseNode> ctor = info.getNodeCtor();
        if (ctor == null) {
            return new AggregateFunctionWithinGroupParseNode(name, args, info);
        } else {
            try {
                return ctor.newInstance(name, args, info);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public HintNode hint(String hint) {
        return new HintNode(hint);
    }

    public InListParseNode inList(List<ParseNode> children, boolean negate) {
        return new InListParseNode(children, negate);
    }

    public ExistsParseNode exists(ParseNode child, boolean negate) {
        return new ExistsParseNode(child, negate);
    }

    public InParseNode in(ParseNode l, ParseNode r, boolean negate, boolean isSubqueryDistinct) {
        return new InParseNode(l, r, negate, isSubqueryDistinct);
    }

    public IsNullParseNode isNull(ParseNode child, boolean negate) {
        return new IsNullParseNode(child, negate);
    }

    public JoinTableNode join(JoinType type, TableNode lhs, TableNode rhs, ParseNode on, boolean singleValueOnly) {
        return new JoinTableNode(type, lhs, rhs, on, singleValueOnly);
    }

    public DerivedTableNode derivedTable (String alias, SelectStatement select) {
        return new DerivedTableNode(alias, select);
    }

    public LikeParseNode like(ParseNode lhs, ParseNode rhs, boolean negate, LikeType likeType) {
        return new LikeParseNode(lhs, rhs, negate, likeType);
    }

    public LiteralParseNode literal(Object value) {
        return new LiteralParseNode(value);
    }

    public LiteralParseNode realNumber(String text) {
        return new LiteralParseNode(new BigDecimal(text, PDataType.DEFAULT_MATH_CONTEXT));
    }
    
    public LiteralParseNode wholeNumber(String text) {
        int length = text.length();
        // We know it'll fit into long, might still fit into int
        if (length <= PDataType.LONG_PRECISION-1) {
            long l = Long.parseLong(text);
            if (l <= Integer.MAX_VALUE) {
                // Fits into int
                return new LiteralParseNode((int)l);
            }
            return new LiteralParseNode(l);
        }
        // Might still fit into long
        BigDecimal d = new BigDecimal(text, PDataType.DEFAULT_MATH_CONTEXT);
        if (d.compareTo(MAX_LONG) <= 0) {
            return new LiteralParseNode(d.longValueExact());
        }
        // Doesn't fit into long
        return new LiteralParseNode(d);
    }

    public LiteralParseNode intOrLong(String text) {
        long l = Long.parseLong(text);
        if (l <= Integer.MAX_VALUE) {
            // Fits into int
            return new LiteralParseNode((int)l);
        }
        return new LiteralParseNode(l);
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
        /*
        Object typedValue = expectedType.toObject(value.toString());
        return new LiteralParseNode(typedValue);
        */
    }

    public LiteralParseNode literal(String value, String sqlTypeName) throws SQLException {
        PDataType expectedType = sqlTypeName == null ? null : PDataType.fromSqlTypeName(SchemaUtil.normalizeIdentifier(sqlTypeName));
        if (expectedType == null || !expectedType.isCoercibleTo(PTimestamp.INSTANCE)) {
            throw TypeMismatchException.newException(expectedType, PTimestamp.INSTANCE);
        }
        Object typedValue = expectedType.toObject(value);
        return new LiteralParseNode(typedValue);
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
    
    public ArrayAnyComparisonNode arrayAny(ParseNode rhs, ComparisonParseNode compareNode) {
        return new ArrayAnyComparisonNode(rhs, compareNode);
    }
    
    public ArrayAllComparisonNode arrayAll(ParseNode rhs, ComparisonParseNode compareNode) {
        return new ArrayAllComparisonNode(rhs, compareNode);
    }

    public ArrayAnyComparisonNode wrapInAny(CompareOp op, ParseNode lhs, ParseNode rhs) {
        return new ArrayAnyComparisonNode(rhs, comparison(op, lhs, elementRef(Arrays.<ParseNode>asList(rhs, literal(1)))));
    }

    public ArrayAllComparisonNode wrapInAll(CompareOp op, ParseNode lhs, ParseNode rhs) {
        return new ArrayAllComparisonNode(rhs, comparison(op, lhs, elementRef(Arrays.<ParseNode>asList(rhs, literal(1)))));
    }

    public ArrayElemRefNode elementRef(List<ParseNode> parseNode) {
        return new ArrayElemRefNode(parseNode);
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

    public ParseNode negate(ParseNode child) {
        // Prevents reparsing of -1 from becoming 1*-1 and 1*1*-1 with each re-parsing
        if (LiteralParseNode.ONE.equals(child) && ((LiteralParseNode)child).getType().isCoercibleTo(
                PLong.INSTANCE)) {
            return LiteralParseNode.MINUS_ONE;
        }
        // Special case to convert Long.MIN_VALUE back to a Long. We can't initially represent it
        // as a Long in the parser because we only represent positive values as constants in the
        // parser, and ABS(Long.MIN_VALUE) is too big to fit into a Long. So we convert it back here.
        if (LiteralParseNode.MIN_LONG_AS_BIG_DECIMAL.equals(child)) {
            return LiteralParseNode.MIN_LONG;
        }
        return new MultiplyParseNode(Arrays.asList(child,LiteralParseNode.MINUS_ONE));
    }

    public NotEqualParseNode notEqual(ParseNode lhs, ParseNode rhs) {
        return new NotEqualParseNode(lhs, rhs);
    }

    public ParseNode not(ParseNode child) {
        if (child instanceof ExistsParseNode) {
            return exists(child.getChildren().get(0), !((ExistsParseNode) child).isNegate());
        }
        
        return new NotParseNode(child);
    }


    public OrParseNode or(List<ParseNode> children) {
        return new OrParseNode(children);
    }


    public OrderByNode orderBy(ParseNode expression, boolean nullsLast, boolean orderAscending) {
        return new OrderByNode(expression, nullsLast, orderAscending);
    }

    public SelectStatement select(TableNode from, HintNode hint, boolean isDistinct, List<AliasedNode> select, ParseNode where,
            List<ParseNode> groupBy, ParseNode having, List<OrderByNode> orderBy, LimitNode limit, int bindCount, boolean isAggregate, 
            boolean hasSequence, List<SelectStatement> selects) {

        return new SelectStatement(from, hint, isDistinct, select, where, groupBy == null ? Collections.<ParseNode>emptyList() : groupBy, having,
                orderBy == null ? Collections.<OrderByNode>emptyList() : orderBy, limit, bindCount, isAggregate, hasSequence, selects == null ? Collections.<SelectStatement>emptyList() : selects);
    } 
    
    public UpsertStatement upsert(NamedTableNode table, HintNode hint, List<ColumnName> columns, List<ParseNode> values, SelectStatement select, int bindCount) {
        return new UpsertStatement(table, hint, columns, values, select, bindCount);
    }

    public DeleteStatement delete(NamedTableNode table, HintNode hint, ParseNode node, List<OrderByNode> orderBy, LimitNode limit, int bindCount) {
        return new DeleteStatement(table, hint, node, orderBy, limit, bindCount);
    }

    public SelectStatement select(SelectStatement statement, ParseNode where) {
        return select(statement.getFrom(), statement.getHint(), statement.isDistinct(), statement.getSelect(), where, statement.getGroupBy(), statement.getHaving(),
                statement.getOrderBy(), statement.getLimit(), statement.getBindCount(), statement.isAggregate(), statement.hasSequence(), statement.getSelects());
    }

    public SelectStatement select(SelectStatement statement, ParseNode where, ParseNode having) {
        return select(statement.getFrom(), statement.getHint(), statement.isDistinct(), statement.getSelect(), where, statement.getGroupBy(), having,
                statement.getOrderBy(), statement.getLimit(), statement.getBindCount(), statement.isAggregate(), statement.hasSequence(), statement.getSelects());
    }
    
    public SelectStatement select(SelectStatement statement, List<AliasedNode> select, ParseNode where, List<ParseNode> groupBy, ParseNode having, List<OrderByNode> orderBy) {
        return select(statement.getFrom(), statement.getHint(), statement.isDistinct(), 
                select, where, groupBy, having, orderBy, statement.getLimit(), statement.getBindCount(), statement.isAggregate(), statement.hasSequence(), statement.getSelects());
    }
    
    public SelectStatement select(SelectStatement statement, TableNode table) {
        return select(table, statement.getHint(), statement.isDistinct(), statement.getSelect(), statement.getWhere(), statement.getGroupBy(),
                statement.getHaving(), statement.getOrderBy(), statement.getLimit(), statement.getBindCount(), statement.isAggregate(),
                statement.hasSequence(), statement.getSelects());
    }

    public SelectStatement select(SelectStatement statement, TableNode table, ParseNode where) {
        return select(table, statement.getHint(), statement.isDistinct(), statement.getSelect(), where, statement.getGroupBy(),
                statement.getHaving(), statement.getOrderBy(), statement.getLimit(), statement.getBindCount(), statement.isAggregate(),
                statement.hasSequence(), statement.getSelects());
    }

    public SelectStatement select(SelectStatement statement, boolean isDistinct, List<AliasedNode> select) {
        return select(statement.getFrom(), statement.getHint(), isDistinct, select, statement.getWhere(), statement.getGroupBy(),
                statement.getHaving(), statement.getOrderBy(), statement.getLimit(), statement.getBindCount(), statement.isAggregate(),
                statement.hasSequence(), statement.getSelects());
    }

    public SelectStatement select(SelectStatement statement, boolean isDistinct, List<AliasedNode> select, ParseNode where) {
        return select(statement.getFrom(), statement.getHint(), isDistinct, select, where, statement.getGroupBy(),
                statement.getHaving(), statement.getOrderBy(), statement.getLimit(), statement.getBindCount(), statement.isAggregate(),
                statement.hasSequence(), statement.getSelects());
    }

    public SelectStatement select(SelectStatement statement, boolean isDistinct, List<AliasedNode> select, ParseNode where, List<ParseNode> groupBy, boolean isAggregate) {
        return select(statement.getFrom(), statement.getHint(), isDistinct, select, where, groupBy,
                statement.getHaving(), statement.getOrderBy(), statement.getLimit(), statement.getBindCount(), isAggregate,
                statement.hasSequence(), statement.getSelects());
    }

    public SelectStatement select(SelectStatement statement, List<OrderByNode> orderBy) {
        return select(statement.getFrom(), statement.getHint(), statement.isDistinct(), statement.getSelect(),
                statement.getWhere(), statement.getGroupBy(), statement.getHaving(), orderBy, statement.getLimit(),
                statement.getBindCount(), statement.isAggregate(), statement.hasSequence(), statement.getSelects());
    }

    public SelectStatement select(SelectStatement statement, HintNode hint) {
        return hint == null || hint.isEmpty() ? statement : select(statement.getFrom(), hint, statement.isDistinct(), statement.getSelect(),
                statement.getWhere(), statement.getGroupBy(), statement.getHaving(), statement.getOrderBy(), statement.getLimit(),
                statement.getBindCount(), statement.isAggregate(), statement.hasSequence(), statement.getSelects());
    }

    public SelectStatement select(SelectStatement statement, HintNode hint, ParseNode where) {
        return select(statement.getFrom(), hint, statement.isDistinct(), statement.getSelect(), where, statement.getGroupBy(),
                statement.getHaving(), statement.getOrderBy(), statement.getLimit(), statement.getBindCount(), statement.isAggregate(),
                statement.hasSequence(), statement.getSelects());
    }

    public SelectStatement select(SelectStatement statement, List<OrderByNode> orderBy, LimitNode limit, int bindCount, boolean isAggregate) {
        return select(statement.getFrom(), statement.getHint(), statement.isDistinct(), statement.getSelect(),
            statement.getWhere(), statement.getGroupBy(), statement.getHaving(), orderBy, limit,
            bindCount, isAggregate || statement.isAggregate(), statement.hasSequence(), statement.getSelects());

    }

    public SelectStatement select(SelectStatement statement, LimitNode limit) {
        return select(statement.getFrom(), statement.getHint(), statement.isDistinct(), statement.getSelect(),
            statement.getWhere(), statement.getGroupBy(), statement.getHaving(), statement.getOrderBy(), limit,
            statement.getBindCount(), statement.isAggregate(), statement.hasSequence(), statement.getSelects());
    }

    public SelectStatement select(SelectStatement statement, List<OrderByNode> orderBy, LimitNode limit) {
        return select(statement.getFrom(), statement.getHint(), statement.isDistinct(), statement.getSelect(),
            statement.getWhere(), statement.getGroupBy(), statement.getHaving(), orderBy, limit,
            statement.getBindCount(), statement.isAggregate(), statement.hasSequence(), statement.getSelects());
    }

    public SelectStatement select(List<SelectStatement> statements, List<OrderByNode> orderBy, LimitNode limit, int bindCount, boolean isAggregate) {
        if (statements.size() == 1)
            return select(statements.get(0), orderBy, limit, bindCount, isAggregate);        

        // Get a list of adjusted aliases from a non-wildcard sub-select if any. 
        // We do not check the number of select nodes among all sub-selects, as 
        // it will be done later at compile stage. Empty or different aliases 
        // are ignored, since they cannot be referred by outer queries.
        List<String> aliases = Lists.<String> newArrayList();
        for (int i = 0; i < statements.size() && aliases.isEmpty(); i++) {
            SelectStatement subselect = statements.get(i);
            if (!subselect.hasWildcard()) {
                for (AliasedNode aliasedNode : subselect.getSelect()) {
                    String alias = aliasedNode.getAlias();
                    if (alias == null) {
                        alias = SchemaUtil.normalizeIdentifier(aliasedNode.getNode().getAlias());
                    }
                    aliases.add(alias == null ? createTempAlias() : alias);
                }
            }
        }

        List<AliasedNode> aliasedNodes;
        if (aliases.isEmpty()) {
            aliasedNodes = Lists.newArrayList(aliasedNode(null, wildcard()));
        } else {
            aliasedNodes = Lists.newArrayListWithExpectedSize(aliases.size());
            for (String alias : aliases) {
                aliasedNodes.add(aliasedNode(alias, column(null, alias, alias)));
            }
        }
        
        return select(null, HintNode.EMPTY_HINT_NODE, false, aliasedNodes, 
                null, null, null, orderBy, limit, bindCount, false, false, statements);
    }

    public SubqueryParseNode subquery(SelectStatement select, boolean expectSingleRow) {
        return new SubqueryParseNode(select, expectSingleRow);
    }

    public LimitNode limit(BindParseNode b) {
        return new LimitNode(b);
    }

    public LimitNode limit(LiteralParseNode l) {
        return new LimitNode(l);
    }
}
