package org.apache.phoenix.calcite;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SemiJoinType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Util;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.calcite.rel.PhoenixRelImplementor;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.CoerceExpression;
import org.apache.phoenix.expression.ComparisonExpression;
import org.apache.phoenix.expression.DateAddExpression;
import org.apache.phoenix.expression.DateSubtractExpression;
import org.apache.phoenix.expression.DecimalAddExpression;
import org.apache.phoenix.expression.DecimalDivideExpression;
import org.apache.phoenix.expression.DecimalMultiplyExpression;
import org.apache.phoenix.expression.DecimalSubtractExpression;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.DoubleAddExpression;
import org.apache.phoenix.expression.DoubleDivideExpression;
import org.apache.phoenix.expression.DoubleMultiplyExpression;
import org.apache.phoenix.expression.DoubleSubtractExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.expression.IsNullExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.LongAddExpression;
import org.apache.phoenix.expression.LongDivideExpression;
import org.apache.phoenix.expression.LongMultiplyExpression;
import org.apache.phoenix.expression.LongSubtractExpression;
import org.apache.phoenix.expression.NotExpression;
import org.apache.phoenix.expression.OrExpression;
import org.apache.phoenix.expression.StringBasedLikeExpression;
import org.apache.phoenix.expression.TimestampAddExpression;
import org.apache.phoenix.expression.TimestampSubtractExpression;
import org.apache.phoenix.expression.function.AbsFunction;
import org.apache.phoenix.expression.function.AggregateFunction;
import org.apache.phoenix.expression.function.CeilDateExpression;
import org.apache.phoenix.expression.function.CeilDecimalExpression;
import org.apache.phoenix.expression.function.CeilFunction;
import org.apache.phoenix.expression.function.CeilTimestampExpression;
import org.apache.phoenix.expression.function.CoalesceFunction;
import org.apache.phoenix.expression.function.CountAggregateFunction;
import org.apache.phoenix.expression.function.CurrentDateFunction;
import org.apache.phoenix.expression.function.CurrentTimeFunction;
import org.apache.phoenix.expression.function.ExpFunction;
import org.apache.phoenix.expression.function.FloorDateExpression;
import org.apache.phoenix.expression.function.FloorDecimalExpression;
import org.apache.phoenix.expression.function.FloorFunction;
import org.apache.phoenix.expression.function.FunctionExpression;
import org.apache.phoenix.expression.function.LnFunction;
import org.apache.phoenix.expression.function.LowerFunction;
import org.apache.phoenix.expression.function.MaxAggregateFunction;
import org.apache.phoenix.expression.function.MinAggregateFunction;
import org.apache.phoenix.expression.function.PowerFunction;
import org.apache.phoenix.expression.function.RoundDecimalExpression;
import org.apache.phoenix.expression.function.RoundTimestampExpression;
import org.apache.phoenix.expression.function.SqrtFunction;
import org.apache.phoenix.expression.function.SumAggregateFunction;
import org.apache.phoenix.expression.function.TrimFunction;
import org.apache.phoenix.expression.function.UDFExpression;
import org.apache.phoenix.expression.function.UpperFunction;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.FunctionParseNode.FunctionClassType;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunctionInfo;
import org.apache.phoenix.parse.JoinTableNode.JoinType;
import org.apache.phoenix.parse.SequenceValueParseNode;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.schema.types.PBinaryArray;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PCharArray;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PUnsignedTimestamp;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.phoenix.util.ExpressionUtil;

/**
 * Utilities for interacting with Calcite.
 */
public class CalciteUtils {
    private CalciteUtils() {}
    
    private static AtomicInteger tempAliasCounter = new AtomicInteger(0);
    private static final FrameworkConfig config;
    static {
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        config = Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.Config.DEFAULT)
            .defaultSchema(rootSchema).build();
    }

    protected static final List<String> TRANSLATED_BUILT_IN_FUNCTIONS = Lists.newArrayList(
            SqrtFunction.NAME,
            PowerFunction.NAME,
            LnFunction.NAME,
            ExpFunction.NAME,
            AbsFunction.NAME,
            CurrentDateFunction.NAME,
            CurrentTimeFunction.NAME,
            LowerFunction.NAME,
            UpperFunction.NAME,
            CoalesceFunction.NAME,
            TrimFunction.NAME,
            CeilFunction.NAME,
            FloorFunction.NAME);

    public static String createTempAlias() {
        return "$" + tempAliasCounter.incrementAndGet();
    }
    
    @SuppressWarnings("rawtypes")
    public static PDataType relDataTypeToPDataType(final RelDataType relDataType) {
        SqlTypeName sqlTypeName = relDataType.getSqlTypeName();
        final boolean isArrayType = sqlTypeName == SqlTypeName.ARRAY;
        if (isArrayType) {
            sqlTypeName = ((ArraySqlType) relDataType).getComponentType().getSqlTypeName();
        }
        if (SqlTypeName.INTERVAL_TYPES.contains(sqlTypeName)) {
            sqlTypeName = SqlTypeName.DECIMAL;
        }
        final int ordinal = sqlTypeName.getJdbcOrdinal();
        if (ordinal >= PDataType.ARRAY_TYPE_BASE) {
            throw new UnsupportedOperationException(
                    "Cannot convert RelDataType: " + relDataType +
                    " to PDataType");
        }
        return PDataType.fromTypeId(ordinal + (isArrayType ? PDataType.ARRAY_TYPE_BASE : 0));
    }

    @SuppressWarnings("rawtypes")
    public static RelDataType pDataTypeToRelDataType(
            final RelDataTypeFactory typeFactory, final PDataType pDataType,
            final Integer maxLength, final Integer scale, final Integer arraySize) {
        final boolean isArrayType = pDataType.isArrayType();
        final PDataType baseType = isArrayType ?
                        PDataType.fromTypeId(pDataType.getSqlType() - PDataType.ARRAY_TYPE_BASE) 
                      : pDataType;
        final int sqlTypeId = baseType.getResultSetSqlType();
        final PDataType normalizedBaseType = PDataType.fromTypeId(sqlTypeId);
        final SqlTypeName sqlTypeName = SqlTypeName.valueOf(normalizedBaseType.getSqlTypeName());
        RelDataType type;
        if (maxLength != null && scale != null) {
            type = typeFactory.createSqlType(sqlTypeName, maxLength, scale);
        } else if (maxLength != null) {
            type = typeFactory.createSqlType(sqlTypeName, maxLength);
        } else {
            type = typeFactory.createSqlType(sqlTypeName);
        }
        if (isArrayType) {
            type = typeFactory.createArrayType(type, arraySize == null ? -1 : arraySize);
        }

        return type;
    }

    public static JoinType convertJoinType(JoinRelType type) {
        JoinType ret = null;
        switch (type) {
        case INNER:
            ret = JoinType.Inner;
            break;
        case LEFT:
            ret = JoinType.Left;
            break;
        case RIGHT:
            ret = JoinType.Right;
            break;
        case FULL:
            ret = JoinType.Full;
            break;
        default:
        }
        
        return ret;
    }
    
    public static JoinType convertSemiJoinType(SemiJoinType type) {
        JoinType ret = null;
        switch (type) {
        case INNER:
            ret = JoinType.Inner;
            break;
        case LEFT:
            ret = JoinType.Left;
            break;
        case SEMI:
            ret = JoinType.Semi;
            break;
        case ANTI:
            ret = JoinType.Anti;
            break;
        default:
        }
        
        return ret;
    }
    
    public static RelCollation reverseCollation(RelCollation collation) {
        if (collation.getFieldCollations().isEmpty())
            return collation;
        
        List<RelFieldCollation> fieldCollations = Lists.newArrayList();
        for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
            Direction dir = null;
            switch (fieldCollation.direction) {
            case ASCENDING:
                dir = Direction.DESCENDING;
                break;
            case DESCENDING:
                dir = Direction.ASCENDING;
                break;
            default:
                assert false : "Shouldn't have come accross non Phoenix directions";
            }
            NullDirection nullDir = null;
            switch (fieldCollation.nullDirection) {
            case FIRST:
                nullDir = NullDirection.LAST;
                break;
            case LAST:
                nullDir = NullDirection.FIRST;
                break;
            default:
                nullDir = NullDirection.UNSPECIFIED;
            }
            fieldCollations.add(new RelFieldCollation(fieldCollation.getFieldIndex(), dir, nullDir));
        }
        return RelCollations.of(fieldCollations);
    }

	private static final Map<SqlKind, ExpressionFactory> EXPRESSION_MAP = Maps
			.newHashMapWithExpectedSize(ExpressionType.values().length);
	private static final ExpressionFactory getFactory(RexNode node) {
		ExpressionFactory eFactory = EXPRESSION_MAP.get(node.getKind());
		if (eFactory == null) {
			throw new UnsupportedOperationException("Unsupported RexNode: "
					+ node);
		}
		return eFactory;
	}
	static {
        EXPRESSION_MAP.put(SqlKind.AND, new ExpressionFactory() {

            @Override
            public Expression newExpression(RexNode node, PhoenixRelImplementor implementor) {
                try {
                    return AndExpression.create(convertChildren((RexCall) node, implementor));
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            
        });
        EXPRESSION_MAP.put(SqlKind.OR, new ExpressionFactory() {

            @Override
            public Expression newExpression(RexNode node, PhoenixRelImplementor implementor) {
                return new OrExpression(convertChildren((RexCall) node, implementor));
            }
            
        });
		EXPRESSION_MAP.put(SqlKind.EQUALS, new ExpressionFactory() {

			@Override
			public Expression newExpression(RexNode node, PhoenixRelImplementor implementor) {
				ImmutableBytesWritable ptr = new ImmutableBytesWritable();
				try {
					return ComparisonExpression.create(CompareOp.EQUAL, convertChildren((RexCall) node, implementor), ptr, implementor.getTableMapping().getPTable().rowKeyOrderOptimizable());
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
			}
			
		});
        EXPRESSION_MAP.put(SqlKind.NOT_EQUALS, new ExpressionFactory() {

            @Override
            public Expression newExpression(RexNode node, PhoenixRelImplementor implementor) {
                ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                try {
                    return ComparisonExpression.create(CompareOp.NOT_EQUAL, convertChildren((RexCall) node, implementor), ptr, implementor.getTableMapping().getPTable().rowKeyOrderOptimizable());
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            
        });
        EXPRESSION_MAP.put(SqlKind.GREATER_THAN, new ExpressionFactory() {

            @Override
            public Expression newExpression(RexNode node, PhoenixRelImplementor implementor) {
                ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                try {
                    return ComparisonExpression.create(CompareOp.GREATER, convertChildren((RexCall) node, implementor), ptr, implementor.getTableMapping().getPTable().rowKeyOrderOptimizable());
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            
        });
        EXPRESSION_MAP.put(SqlKind.GREATER_THAN_OR_EQUAL, new ExpressionFactory() {

            @Override
            public Expression newExpression(RexNode node, PhoenixRelImplementor implementor) {
                ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                try {
                    return ComparisonExpression.create(CompareOp.GREATER_OR_EQUAL, convertChildren((RexCall) node, implementor), ptr, implementor.getTableMapping().getPTable().rowKeyOrderOptimizable());
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            
        });
        EXPRESSION_MAP.put(SqlKind.LESS_THAN, new ExpressionFactory() {

            @Override
            public Expression newExpression(RexNode node, PhoenixRelImplementor implementor) {
                ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                try {
                    return ComparisonExpression.create(CompareOp.LESS, convertChildren((RexCall) node, implementor), ptr, implementor.getTableMapping().getPTable().rowKeyOrderOptimizable());
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            
        });
        EXPRESSION_MAP.put(SqlKind.LESS_THAN_OR_EQUAL, new ExpressionFactory() {

            @Override
            public Expression newExpression(RexNode node, PhoenixRelImplementor implementor) {
                ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                try {
                    return ComparisonExpression.create(CompareOp.LESS_OR_EQUAL, convertChildren((RexCall) node, implementor), ptr, implementor.getTableMapping().getPTable().rowKeyOrderOptimizable());
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            
        });
        EXPRESSION_MAP.put(SqlKind.PLUS, new ExpressionFactory() {

            @SuppressWarnings("rawtypes")
            @Override
            public Expression newExpression(RexNode node, PhoenixRelImplementor implementor) {
                try {
                    List<Expression> children = convertChildren((RexCall) node, implementor);
                    Expression expr = null;
                    boolean foundDate = false;
                    Determinism determinism = Determinism.ALWAYS;
                    PDataType theType = null;
                    for(int i = 0; i < children.size(); i++) {
                        Expression e = children.get(i);
                        determinism = determinism.combine(e.getDeterminism());
                        PDataType type = e.getDataType();
                        if (type == null) {
                            continue; 
                        } else if (type.isCoercibleTo(PTimestamp.INSTANCE)) {
                            if (foundDate) {
                                throw TypeMismatchException.newException(type, node.toString());
                            }
                            if (theType == null || (theType != PTimestamp.INSTANCE && theType != PUnsignedTimestamp.INSTANCE)) {
                                theType = type;
                            }
                            foundDate = true;
                        }else if (type == PDecimal.INSTANCE) {
                            if (theType == null || !theType.isCoercibleTo(PTimestamp.INSTANCE)) {
                                theType = PDecimal.INSTANCE;
                            }
                        } else if (type.isCoercibleTo(PLong.INSTANCE)) {
                            if (theType == null) {
                                theType = PLong.INSTANCE;
                            }
                        } else if (type.isCoercibleTo(PDouble.INSTANCE)) {
                            if (theType == null) {
                                theType = PDouble.INSTANCE;
                            }
                        } else {
                            throw TypeMismatchException.newException(type, node.toString());
                        }
                    }
                    if (theType == PDecimal.INSTANCE) {
                        expr = new DecimalAddExpression(children);
                    } else if (theType == PLong.INSTANCE) {
                        expr = new LongAddExpression(children);
                    } else if (theType == PDouble.INSTANCE) {
                        expr = new DoubleAddExpression(children);
                    } else if (theType == null) {
                        expr = LiteralExpression.newConstant(null, theType, determinism);
                    } else if (theType == PTimestamp.INSTANCE || theType == PUnsignedTimestamp.INSTANCE) {
                        expr = new TimestampAddExpression(children);
                    } else if (theType.isCoercibleTo(PDate.INSTANCE)) {
                        expr = new DateAddExpression(children);
                    } else {
                        throw TypeMismatchException.newException(theType, node.toString());
                    }
                    
                    PDataType targetType = relDataTypeToPDataType(node.getType());
                    return cast(targetType, null, expr, implementor);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            
        });
        EXPRESSION_MAP.put(SqlKind.MINUS, new ExpressionFactory() {

            @SuppressWarnings("rawtypes")
            @Override
            public Expression newExpression(RexNode node, PhoenixRelImplementor implementor) {
                try {
                    List<Expression> children = convertChildren((RexCall) node, implementor);
                    Expression expr = null;
                    int i = 0;
                    PDataType theType = null;
                    Expression e1 = children.get(0);
                    Expression e2 = children.get(1);
                    Determinism determinism = e1.getDeterminism().combine(e2.getDeterminism());
                    PDataType type1 = e1.getDataType();
                    PDataType type2 = e2.getDataType();
                    // TODO: simplify this special case for DATE conversion
                    /**
                     * For date1-date2, we want to coerce to a LONG because this
                     * cannot be compared against another date. It has essentially
                     * become a number. For date1-5, we want to preserve the DATE
                     * type because this can still be compared against another date
                     * and cannot be multiplied or divided. Any other time occurs is
                     * an error. For example, 5-date1 is an error. The nulls occur if
                     * we have bind variables.
                     */
                    boolean isType1Date = 
                            type1 != null 
                            && type1 != PTimestamp.INSTANCE
                            && type1 != PUnsignedTimestamp.INSTANCE
                            && type1.isCoercibleTo(PDate.INSTANCE);
                    boolean isType2Date = 
                            type2 != null
                            && type2 != PTimestamp.INSTANCE
                            && type2 != PUnsignedTimestamp.INSTANCE
                            && type2.isCoercibleTo(PDate.INSTANCE);
                    if (isType1Date || isType2Date) {
                        if (isType1Date && isType2Date) {
                            i = 2;
                            theType = PDecimal.INSTANCE;
                        } else if (isType1Date && type2 != null
                                && type2.isCoercibleTo(PDecimal.INSTANCE)) {
                            i = 2;
                            theType = PDate.INSTANCE;
                        } else if (type1 == null || type2 == null) {
                            /*
                             * FIXME: Could be either a Date or BigDecimal, but we
                             * don't know if we're comparing to a date or a number
                             * which would be disambiguate it.
                             */
                            i = 2;
                            theType = null;
                        }
                    } else if(type1 == PTimestamp.INSTANCE || type2 == PTimestamp.INSTANCE) {
                        i = 2;
                        theType = PTimestamp.INSTANCE;
                    } else if(type1 == PUnsignedTimestamp.INSTANCE || type2 == PUnsignedTimestamp.INSTANCE) {
                        i = 2;
                        theType = PUnsignedTimestamp.INSTANCE;
                    }
                    
                    for (; i < children.size(); i++) {
                        // This logic finds the common type to which all child types are coercible
                        // without losing precision.
                        Expression e = children.get(i);
                        determinism = determinism.combine(e.getDeterminism());
                        PDataType type = e.getDataType();
                        if (type == null) {
                            continue;
                        } else if (type.isCoercibleTo(PLong.INSTANCE)) {
                            if (theType == null) {
                                theType = PLong.INSTANCE;
                            }
                        } else if (type == PDecimal.INSTANCE) {
                            // Coerce return type to DECIMAL from LONG or DOUBLE if DECIMAL child found,
                            // unless we're doing date arithmetic.
                            if (theType == null
                                    || !theType.isCoercibleTo(PDate.INSTANCE)) {
                                theType = PDecimal.INSTANCE;
                            }
                        } else if (type.isCoercibleTo(PDouble.INSTANCE)) {
                            // Coerce return type to DOUBLE from LONG if DOUBLE child found,
                            // unless we're doing date arithmetic or we've found another child of type DECIMAL
                            if (theType == null
                                    || (theType != PDecimal.INSTANCE && !theType.isCoercibleTo(PDate.INSTANCE) )) {
                                theType = PDouble.INSTANCE;
                            }
                        } else {
                            throw TypeMismatchException.newException(type, node.toString());
                        }
                    }
                    if (theType == PDecimal.INSTANCE) {
                        expr = new DecimalSubtractExpression(children);
                    } else if (theType == PLong.INSTANCE) {
                        expr = new LongSubtractExpression(children);
                    } else if (theType == PDouble.INSTANCE) {
                        expr = new DoubleSubtractExpression(children);
                    } else if (theType == null) {
                        expr = LiteralExpression.newConstant(null, theType, determinism);
                    } else if (theType == PTimestamp.INSTANCE || theType == PUnsignedTimestamp.INSTANCE) {
                        expr = new TimestampSubtractExpression(children);
                    } else if (theType.isCoercibleTo(PDate.INSTANCE)) {
                        expr = new DateSubtractExpression(children);
                    } else {
                        throw TypeMismatchException.newException(theType, node.toString());
                    }
                    PDataType targetType = relDataTypeToPDataType(node.getType());
                    return cast(targetType, null, expr, implementor);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            
        });
        EXPRESSION_MAP.put(SqlKind.TIMES, new ExpressionFactory() {

            @SuppressWarnings("rawtypes")
            @Override
            public Expression newExpression(RexNode node, PhoenixRelImplementor implementor) {
                try {
                    List<Expression> children = convertChildren((RexCall) node, implementor);
                    Expression expr = null;
                    PDataType theType = null;
                    Determinism determinism = Determinism.ALWAYS;
                    for(int i = 0; i < children.size(); i++) {
                        Expression e = children.get(i);
                        determinism = determinism.combine(e.getDeterminism());
                        PDataType type = e.getDataType();
                        if (type == null) {
                            continue;
                        } else if (type == PDecimal.INSTANCE) {
                            theType = PDecimal.INSTANCE;
                        } else if (type.isCoercibleTo(PLong.INSTANCE)) {
                            if (theType == null) {
                                theType = PLong.INSTANCE;
                            }
                        } else if (type.isCoercibleTo(PDouble.INSTANCE)) {
                            if (theType == null) {
                                theType = PDouble.INSTANCE;
                            }
                        } else {
                            throw TypeMismatchException.newException(type, node.toString());
                        }
                    }
                    if (theType == PDecimal.INSTANCE) {
                        expr = new DecimalMultiplyExpression(children);
                    } else if (theType == PLong.INSTANCE) {
                        expr = new LongMultiplyExpression(children);
                    } else if (theType == PDouble.INSTANCE) {
                        expr = new DoubleMultiplyExpression(children);
                    } else {
                        expr = LiteralExpression.newConstant(null, theType, determinism);
                    }
                    PDataType targetType = relDataTypeToPDataType(node.getType());
                    return cast(targetType, null, expr, implementor);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            
        });
        EXPRESSION_MAP.put(SqlKind.DIVIDE, new ExpressionFactory() {

            @SuppressWarnings("rawtypes")
            @Override
            public Expression newExpression(RexNode node, PhoenixRelImplementor implementor) {
                try {
                    List<Expression> children = convertChildren((RexCall) node, implementor);
                    Expression expr = null;
                    PDataType theType = null;
                    Determinism determinism = Determinism.ALWAYS;
                    for(int i = 0; i < children.size(); i++) {
                        Expression e = children.get(i);
                        determinism = determinism.combine(e.getDeterminism());
                        PDataType type = e.getDataType();
                        if (type == null) {
                            continue;
                        } else if (type == PDecimal.INSTANCE) {
                            theType = PDecimal.INSTANCE;
                        } else if (type.isCoercibleTo(PLong.INSTANCE)) {
                            if (theType == null) {
                                theType = PLong.INSTANCE;
                            }
                        } else if (type.isCoercibleTo(PDouble.INSTANCE)) {
                            if (theType == null) {
                                theType = PDouble.INSTANCE;
                            }
                        } else {
                            throw TypeMismatchException.newException(type, node.toString());
                        }
                    }
                    if (theType == PDecimal.INSTANCE) {
                        expr = new DecimalDivideExpression( children);
                    } else if (theType == PLong.INSTANCE) {
                        expr = new LongDivideExpression( children);
                    } else if (theType == PDouble.INSTANCE) {
                        expr = new DoubleDivideExpression(children);
                    } else {
                        expr = LiteralExpression.newConstant(null, theType, determinism);
                    }
                    PDataType targetType = relDataTypeToPDataType(node.getType());
                    return cast(targetType, null, expr, implementor);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            
        });
		EXPRESSION_MAP.put(SqlKind.LITERAL, new ExpressionFactory() {

			@SuppressWarnings("rawtypes")
            @Override
			public Expression newExpression(RexNode node, PhoenixRelImplementor implementor) {
				RexLiteral lit = (RexLiteral) node;
                PDataType targetType = relDataTypeToPDataType(node.getType());
				Object o = lit.getValue();
				if (o instanceof NlsString) {
				    o = ((NlsString) o).getValue();
				} else if (o instanceof ByteString) {
				    o = ((ByteString) o).getBytes();
				} else if (o instanceof GregorianCalendar) {
				    o = new Timestamp(((GregorianCalendar) o).getTimeInMillis());
				}
				try {
                    return LiteralExpression.newConstant(o, targetType);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
			}
			
		});
		EXPRESSION_MAP.put(SqlKind.INPUT_REF, new ExpressionFactory() {

			@Override
			public Expression newExpression(RexNode node, PhoenixRelImplementor implementor) {
				RexInputRef ref = (RexInputRef) node;
				int index = ref.getIndex();
				return implementor.newColumnExpression(index);
			}
			
		});
		EXPRESSION_MAP.put(SqlKind.FIELD_ACCESS, new ExpressionFactory() {
            @SuppressWarnings("rawtypes")
            @Override
            public Expression newExpression(RexNode node, PhoenixRelImplementor implementor) {
                RexFieldAccess fieldAccess = (RexFieldAccess) node;
                RexNode refExpr = fieldAccess.getReferenceExpr();
                if (refExpr.getKind() != SqlKind.CORREL_VARIABLE) {
                    throw new UnsupportedOperationException("Non-correl-variable as reference expression of RexFieldAccess.");
                }
                String varId = ((RexCorrelVariable) refExpr).getName();
                int index = fieldAccess.getField().getIndex();
                PDataType type = relDataTypeToPDataType(node.getType());
                return implementor.newFieldAccessExpression(varId, index, type);
            }		    
		});
        EXPRESSION_MAP.put(SqlKind.DYNAMIC_PARAM, new ExpressionFactory() {
            @SuppressWarnings("rawtypes")
            @Override
            public Expression newExpression(RexNode node, PhoenixRelImplementor implementor) {
                RexDynamicParam param = (RexDynamicParam) node;
                int index = param.getIndex();
                PDataType type = relDataTypeToPDataType(node.getType());
                Integer maxLength =
                        (type == PChar.INSTANCE
                            || type == PCharArray.INSTANCE
                            || type == PBinary.INSTANCE
                            || type == PBinaryArray.INSTANCE) ?
                        node.getType().getPrecision() : null;
                return implementor.newBindParameterExpression(index, type, maxLength);
            }
        });
		EXPRESSION_MAP.put(SqlKind.CAST, new ExpressionFactory() {

            @SuppressWarnings("rawtypes")
            @Override
            public Expression newExpression(RexNode node,
                    PhoenixRelImplementor implementor) {                
                List<Expression> children = convertChildren((RexCall) node, implementor);
                PDataType targetType = relDataTypeToPDataType(node.getType());
                Integer maxLength =
                        (targetType == PChar.INSTANCE
                            || targetType == PCharArray.INSTANCE
                            || targetType == PBinary.INSTANCE
                            || targetType == PBinaryArray.INSTANCE) ?
                        node.getType().getPrecision() : null;
                try {
                    return cast(targetType, maxLength, children.get(0), implementor);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        EXPRESSION_MAP.put(SqlKind.DEFAULT, new ExpressionFactory() {
            @Override
            public Expression newExpression(RexNode node, PhoenixRelImplementor implementor) {
                return null;
            }
        });
        EXPRESSION_MAP.put(SqlKind.OTHER_FUNCTION, new ExpressionFactory() {
            @Override
            public Expression newExpression(RexNode node,
                    PhoenixRelImplementor implementor) {
                RexCall call = (RexCall) node;
                List<Expression> children = convertChildren(call, implementor);
                SqlOperator op = call.getOperator();
                try {
                    if (op instanceof SqlUserDefinedFunction) {
                        SqlUserDefinedFunction udf = (SqlUserDefinedFunction) op;
                        Function func = udf.getFunction();
                        if (func instanceof PhoenixScalarFunction) {
                            PhoenixScalarFunction scalarFunc = (PhoenixScalarFunction) func;
                            BuiltInFunctionInfo info = scalarFunc.getParseInfo() != null ? scalarFunc.getParseInfo() : new BuiltInFunctionInfo(scalarFunc.getFunctionInfo());
                            if (info.getArgs().length > children.size()) {
                                List<Expression> moreChildren = new ArrayList<Expression>(children);
                                for (int i = children.size(); i < info.getArgs().length; i++) {
                                    if(info.getArgs()[i].getDefaultValue() != null) {
                                        moreChildren.add(info.getArgs()[i].getDefaultValue());
                                    }
                                }
                                children = moreChildren;
                            }
                            for(int i = 0; i < children.size(); i++) {
                                FunctionParseNode.validateFunctionArguement(info, i, children.get(i));
                            }
                            if(scalarFunc.getParseInfo() != null){
                                BuiltInFunctionInfo parseInfo = scalarFunc.getParseInfo();
                                try {
                                    if(parseInfo.getClassType() == FunctionClassType.PARENT || parseInfo.getClassType() == FunctionClassType.ALIAS){
                                        try {
                                            return (Expression) parseInfo.getFunc().getDeclaredMethod("create", List.class).invoke(null, children);
                                        } catch (Exception e){
                                            return (Expression) parseInfo.getFunc().getDeclaredMethod("create", List.class, StatementContext.class).invoke(null, children, implementor.getStatementContext());
                                        }
                                    }
                                    if(parseInfo.getClassType() == FunctionClassType.NONE){
                                        try {
                                            return (Expression) parseInfo.getFunc().getDeclaredConstructor(List.class).newInstance(children);
                                        } catch (Exception e){
                                            return (Expression) parseInfo.getFunc().getDeclaredConstructor(List.class, StatementContext.class).newInstance(children, implementor.getStatementContext());
                                        }
                                    }
                                } catch (Exception e) {throw new RuntimeException ("Failed to create builtin function " + parseInfo.getName(), e);}
                            }
                            return new UDFExpression(children, scalarFunc.getFunctionInfo());
                        }
                    } else if (op == SqlStdOperatorTable.SQRT) {
                        return new SqrtFunction(children);
                    } else if (op == SqlStdOperatorTable.POWER) {
                        return new PowerFunction(children);
                    } else if (op == SqlStdOperatorTable.LN) {
                        return new LnFunction(children);
                    } else if (op == SqlStdOperatorTable.EXP) {
                        return new ExpFunction(children);
                    } else if (op == SqlStdOperatorTable.ABS) {
                        return new AbsFunction(children);
                    } else if (op == SqlStdOperatorTable.CURRENT_DATE) {
                        return new CurrentDateFunction();
                    } else if (op == SqlStdOperatorTable.CURRENT_TIME) {
                        return new CurrentTimeFunction();
                    } else if (op == SqlStdOperatorTable.LOWER) {
                        return new LowerFunction(children);
                    } else if (op == SqlStdOperatorTable.UPPER) {
                        return new UpperFunction(children);
                    } else if (op == SqlStdOperatorTable.COALESCE) {
                        return new CoalesceFunction(children);
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }

                throw new UnsupportedOperationException(
                        "Unsupported SqlFunction: " + op.getName());
            }
		});
        EXPRESSION_MAP.put(SqlKind.NOT, new ExpressionFactory() {
            @Override
            public Expression newExpression(RexNode node, PhoenixRelImplementor implementor) {
                return new NotExpression(convertChildren((RexCall) node, implementor));
            }
        });
        EXPRESSION_MAP.put(SqlKind.IS_TRUE, new ExpressionFactory() {
            @Override
            public Expression newExpression(RexNode node, PhoenixRelImplementor implementor) {
                List<Expression> children = convertChildren((RexCall) node, implementor);
                return children.get(0);
            }
        });
        EXPRESSION_MAP.put(SqlKind.IS_NOT_TRUE, new ExpressionFactory() {
            @Override
            public Expression newExpression(RexNode node, PhoenixRelImplementor implementor) {
                return new NotExpression(convertChildren((RexCall) node, implementor));
            }
        });
        EXPRESSION_MAP.put(SqlKind.IS_FALSE, new ExpressionFactory() {
            @Override
            public Expression newExpression(RexNode node, PhoenixRelImplementor implementor) {
                return new NotExpression(convertChildren((RexCall) node, implementor));
            }
        });
        EXPRESSION_MAP.put(SqlKind.IS_NOT_FALSE, new ExpressionFactory() {
            @Override
            public Expression newExpression(RexNode node, PhoenixRelImplementor implementor) {
                List<Expression> children = convertChildren((RexCall) node, implementor);
                return children.get(0);
            }
        });
        //TODO different kind of LikeExpression based on configuration
        EXPRESSION_MAP.put(SqlKind.LIKE, new ExpressionFactory() {
            @Override
            public Expression newExpression(RexNode node, PhoenixRelImplementor implementor) {
                List<Expression> children = convertChildren((RexCall) node, implementor);
                return new StringBasedLikeExpression(children);
            }
        });
        EXPRESSION_MAP.put(SqlKind.IS_NULL, new ExpressionFactory() {
            @Override
            public Expression newExpression(RexNode node, PhoenixRelImplementor implementor) {
                return new IsNullExpression(convertChildren((RexCall) node, implementor), false);
            }
        });
        EXPRESSION_MAP.put(SqlKind.IS_NOT_NULL, new ExpressionFactory() {
            @Override
            public Expression newExpression(RexNode node, PhoenixRelImplementor implementor) {
                return new IsNullExpression(convertChildren((RexCall) node, implementor), true);
            }
        });
        EXPRESSION_MAP.put(SqlKind.TRIM, new ExpressionFactory() {
            @Override
            public Expression newExpression(RexNode node, PhoenixRelImplementor implementor) {
                //TODO Phoenix only support separate arguments.
                try {
                    return new TrimFunction(convertChildren((RexCall) node, implementor));
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        EXPRESSION_MAP.put(SqlKind.CEIL, new ExpressionFactory() {
            @SuppressWarnings("rawtypes")
            @Override
            public Expression newExpression(RexNode node, PhoenixRelImplementor implementor) {
                //TODO Phoenix only support separate arguments.
                List<Expression> children = convertChildren((RexCall) node, implementor);
                final Expression firstChild = children.get(0);
                final PDataType firstChildDataType = firstChild.getDataType();
                try {
                    if (firstChildDataType.isCoercibleTo(PDate.INSTANCE)) {
                        return CeilDateExpression.create(children);
                    } else if (firstChildDataType == PTimestamp.INSTANCE
                            || firstChildDataType == PUnsignedTimestamp.INSTANCE) {
                        return CeilTimestampExpression.create(children);
                    } else if (firstChildDataType.isCoercibleTo(PDecimal.INSTANCE)) {
                        return CeilDecimalExpression.create(children);
                    } else {
                        throw TypeMismatchException.newException(firstChildDataType, "1");
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        EXPRESSION_MAP.put(SqlKind.FLOOR, new ExpressionFactory() {
            @SuppressWarnings("rawtypes")
            @Override
            public Expression newExpression(RexNode node, PhoenixRelImplementor implementor) {
                // TODO Phoenix only support separate arguments.
                List<Expression> children = convertChildren((RexCall) node, implementor);
                final Expression firstChild = children.get(0);
                final PDataType firstChildDataType = firstChild.getDataType();
                try {
                    if (firstChildDataType.isCoercibleTo(PTimestamp.INSTANCE)) {
                        return FloorDateExpression.create(children);
                    } else if (firstChildDataType.isCoercibleTo(PDecimal.INSTANCE)) {
                        return FloorDecimalExpression.create(children);
                    } else {
                        throw TypeMismatchException.newException(firstChildDataType, "1");
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        EXPRESSION_MAP.put(SqlKind.CURRENT_VALUE, new ExpressionFactory() {
            @Override
            public Expression newExpression(RexNode node, PhoenixRelImplementor implementor) {
                RexCall call = (RexCall) node;
                RexLiteral operand = (RexLiteral) call.getOperands().get(0);
                List<String> name = Util.stringToList((String) operand.getValue2());
                RelOptTable table = Prepare.CatalogReader.THREAD_LOCAL.get().getTable(name);
                PhoenixSequence seq = table.unwrap(PhoenixSequence.class);
                return implementor.newSequenceExpression(seq, SequenceValueParseNode.Op.CURRENT_VALUE);
            }
        });
        EXPRESSION_MAP.put(SqlKind.NEXT_VALUE, new ExpressionFactory() {
            @Override
            public Expression newExpression(RexNode node, PhoenixRelImplementor implementor) {
                RexCall call = (RexCall) node;
                RexLiteral operand = (RexLiteral) call.getOperands().get(0);
                List<String> name = Util.stringToList((String) operand.getValue2());
                RelOptTable table = Prepare.CatalogReader.THREAD_LOCAL.get().getTable(name);
                PhoenixSequence seq = table.unwrap(PhoenixSequence.class);
                return implementor.newSequenceExpression(seq, SequenceValueParseNode.Op.NEXT_VALUE);
            }
        });
        // TODO: SqlKind.CASE
	}
	
    private static final Map<String, FunctionFactory> FUNCTION_MAP = Maps
            .newHashMapWithExpectedSize(ExpressionType.values().length);
    private static final FunctionFactory getFactory(SqlFunction func) {
        FunctionFactory fFactory = FUNCTION_MAP.get(func.getName());
        if (fFactory == null) {
            throw new UnsupportedOperationException("Unsupported SqlFunction: "
                    + func);
        }
        return fFactory;
    }
    static {
        FUNCTION_MAP.put("COUNT", new FunctionFactory() {
            @Override
            public FunctionExpression newFunction(SqlFunction sqlFunc,
                    List<Expression> args) {
                if (args.isEmpty()) {
                    args = Lists.asList(LiteralExpression.newConstant(1), new Expression[0]);
                }
                return new CountAggregateFunction(args);
            }
        });
        FUNCTION_MAP.put("$SUM0", new FunctionFactory() {
            @Override
            public FunctionExpression newFunction(SqlFunction sqlFunc,
                    List<Expression> args) {
                return new SumAggregateFunction(args);
            }
        });
        FUNCTION_MAP.put("SUM", new FunctionFactory() {
            @Override
            public FunctionExpression newFunction(SqlFunction sqlFunc,
                    List<Expression> args) {
                return new SumAggregateFunction(args);
            }
        });
        FUNCTION_MAP.put("MAX", new FunctionFactory() {
            @Override
            public FunctionExpression newFunction(SqlFunction sqlFunc,
                    List<Expression> args) {
                return new MaxAggregateFunction(args, null);
            }
        });
        FUNCTION_MAP.put("MIN", new FunctionFactory() {
            @Override
            public FunctionExpression newFunction(SqlFunction sqlFunc,
                    List<Expression> args) {
                return new MinAggregateFunction(args, null);
            }
        });
    }
    
    private static List<Expression> convertChildren(RexCall call, PhoenixRelImplementor implementor) {
        List<Expression> children = Lists.newArrayListWithExpectedSize(call.getOperands().size());
        for (RexNode op : call.getOperands()) {
            Expression child = getFactory(op).newExpression(op, implementor);
            if(child != null) {
                children.add(child);
            }
        }
        return children;
    }
    
    @SuppressWarnings("rawtypes")
    private static Expression cast(PDataType targetDataType, Integer maxLength, Expression childExpr, PhoenixRelImplementor implementor) throws SQLException {
        PDataType fromDataType = childExpr.getDataType();
        
        Expression expr = childExpr;
        if(fromDataType != null && implementor.getTableMapping().getPTable().getType() != PTableType.INDEX) {
            expr =  convertToRoundExpressionIfNeeded(fromDataType, targetDataType, childExpr);
        }
        return CoerceExpression.create(expr, targetDataType, SortOrder.getDefault(), maxLength, implementor.getTableMapping().getPTable().rowKeyOrderOptimizable());
    }
    
    @SuppressWarnings("rawtypes")
    private static Expression convertToRoundExpressionIfNeeded(PDataType fromDataType, PDataType targetDataType, Expression expr) throws SQLException {
        if(fromDataType == targetDataType) {
            return expr;
        } else if((fromDataType == PDecimal.INSTANCE || fromDataType == PTimestamp.INSTANCE || fromDataType == PUnsignedTimestamp.INSTANCE) && targetDataType.isCoercibleTo(
          PLong.INSTANCE)) {
            return RoundDecimalExpression.create(Arrays.asList(expr));
        } else if((fromDataType == PDecimal.INSTANCE || fromDataType == PTimestamp.INSTANCE || fromDataType == PUnsignedTimestamp.INSTANCE) && targetDataType.isCoercibleTo(
          PDate.INSTANCE)) {
            return RoundTimestampExpression.create(Arrays.asList(expr));
        } else if(fromDataType.isCastableTo(targetDataType)) {
            return expr;
        } else {
            throw TypeMismatchException.newException(fromDataType, targetDataType, expr.toString());
        }
    }

    public static boolean isExpressionSupported(RexNode node) {
        try {
            getFactory(node);
        } catch (UnsupportedOperationException e) {
            return false;
        }
        if (node instanceof RexCall) {
            for (RexNode op : ((RexCall) node).getOperands()) {
                if (!isExpressionSupported(op)) {
                    return false;
                }
            }
        }
        
        return true;
    }
    
    public static boolean isAggregateFunctionSupported(SqlAggFunction aggFunc) {
        try {
            getFactory(aggFunc);
        } catch (UnsupportedOperationException e) {
            return false;
        }

        return true;
    }

	public static Expression toExpression(RexNode node, PhoenixRelImplementor implementor) {
		ExpressionFactory eFactory = getFactory(node);
		Expression expression = eFactory.newExpression(node, implementor);
		return expression;
	}
	
	public static AggregateFunction toAggregateFunction(SqlAggFunction aggFunc, List<Integer> args, PhoenixRelImplementor implementor) {
	    FunctionFactory fFactory = getFactory(aggFunc);
	    List<Expression> exprs = Lists.newArrayListWithExpectedSize(args.size());
	    for (Integer index : args) {
	        exprs.add(implementor.newColumnExpression(index));
	    }
	    
	    return (AggregateFunction) (fFactory.newFunction(aggFunc, exprs));
	}
	
	public static interface ExpressionFactory {
		public Expression newExpression(RexNode node, PhoenixRelImplementor implementor);
	}
	
	public static interface FunctionFactory {
	    public FunctionExpression newFunction(SqlFunction sqlFunc, List<Expression> args);
	}
	
	public static boolean hasSequenceValueCall(Project project) {
		SequenceValueFinder seqFinder = new SequenceValueFinder();
		for (RexNode node : project.getProjects()) {
			node.accept(seqFinder);
			if (seqFinder.sequenceValueCall != null) {
				return true;
			}
		}
		
		return false;
	}
	
	public static PhoenixSequence findSequence(Project project) {
        SequenceValueFinder seqFinder = new SequenceValueFinder();
        for (RexNode node : project.getProjects()) {
            node.accept(seqFinder);
            if (seqFinder.sequenceValueCall != null) {
                RexLiteral operand =
                		(RexLiteral) seqFinder.sequenceValueCall.getOperands().get(0);
                List<String> name = Util.stringToList((String) operand.getValue2());
                RelOptTable table = Prepare.CatalogReader.THREAD_LOCAL.get().getTable(name);
                return table.unwrap(PhoenixSequence.class);
            }
        }
        
        return null;
	}
    
    private static class SequenceValueFinder extends RexVisitorImpl<Void> {
        private RexCall sequenceValueCall;

        private SequenceValueFinder() {
            super(true);
        }
        
        public Void visitCall(RexCall call) {
            if (sequenceValueCall == null
                    && (call.getKind() == SqlKind.CURRENT_VALUE
                        || call.getKind() == SqlKind.NEXT_VALUE)) {
                sequenceValueCall = call;
            }
            return null;
        }
    }

    public static Object convertSqlLiteral(SqlLiteral literal, PhoenixRelImplementor implementor) {
        try {
            final Planner planner = Frameworks.getPlanner(config);

            SqlParserPos POS = SqlParserPos.ZERO;
            final SqlNodeList selectList =
                    new SqlNodeList(
                            Collections.singletonList(literal),
                            SqlParserPos.ZERO);


            String sql = new SqlSelect(POS, SqlNodeList.EMPTY, selectList, null, null, null, null,
                    SqlNodeList.EMPTY, null, null, null).toString();
            SqlNode sqlNode = planner.parse(sql);
            sqlNode = planner.validate(sqlNode);
            RelNode relNode = planner.rel(sqlNode).rel;

            assert relNode instanceof Project;
            Project proj = (Project) relNode;
            assert proj.getChildExps().size() == 1;
            RexNode rex = proj.getChildExps().get(0);

            Expression e = CalciteUtils.toExpression(rex, implementor);
            ImmutableBytesWritable ptr = new ImmutableBytesWritable();
            e = ExpressionUtil.getConstantExpression(e, ptr);
            return e.getDataType().toObject(ptr);
        } catch (Exception ex){
            throw new RuntimeException("Could not convert literal " + literal.getValue()
                    + " to its object type.", ex);
        }
    }

    public static SQLException unwrapSqlException(SQLException root){
        Exception e = root;
        while(e.getCause() != null){
            e = (Exception) e.getCause();
            if(e instanceof RuntimeException && e.getCause() instanceof SQLException) {
                return (SQLException) e.getCause();
            }
            if(e instanceof SQLException){
                return (SQLException) e;
            }
        }
        return root;
    }
}
