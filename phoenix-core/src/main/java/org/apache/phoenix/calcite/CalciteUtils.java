package org.apache.phoenix.calcite;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.calcite.PhoenixRel.Implementor;
import org.apache.phoenix.calcite.PhoenixRel.PlanType;
import org.apache.phoenix.expression.ComparisonExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.function.AggregateFunction;
import org.apache.phoenix.expression.function.CountAggregateFunction;
import org.apache.phoenix.expression.function.FunctionExpression;
import org.apache.phoenix.expression.function.MaxAggregateFunction;
import org.apache.phoenix.expression.function.SumAggregateFunction;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Utilities for interacting with Calcite.
 */
public class CalciteUtils {
    private CalciteUtils() {}
    
    private static AtomicInteger tempAliasCounter = new AtomicInteger(0);
  
    public static String createTempAlias() {
        return "$" + tempAliasCounter.incrementAndGet();
    }
    
    public static RelNode getBestRel(RelNode rel) {
        if (rel instanceof RelSubset)
            return ((RelSubset) rel).getBest();
        
        return rel;
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
		EXPRESSION_MAP.put(SqlKind.EQUALS, new ExpressionFactory() {

			@Override
			public Expression newExpression(RexNode node, Implementor implementor) {
				RexCall call = (RexCall) node;
				List<Expression> children = Lists.newArrayListWithExpectedSize(call.getOperands().size());
				for (RexNode op : call.getOperands()) {
					Expression child = getFactory(op).newExpression(op, implementor);
					children.add(child);
				}
				ImmutableBytesWritable ptr = new ImmutableBytesWritable();
				try {
					return ComparisonExpression.create(CompareOp.EQUAL, children, ptr);
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
			}
			
		});
		EXPRESSION_MAP.put(SqlKind.LITERAL, new ExpressionFactory() {

			@Override
			public Expression newExpression(RexNode node, Implementor implementor) {
				RexLiteral lit = (RexLiteral) node;
				Object o = lit.getValue2();
				return LiteralExpression.newConstant(o);
			}
			
		});
		EXPRESSION_MAP.put(SqlKind.INPUT_REF, new ExpressionFactory() {

			@Override
			public Expression newExpression(RexNode node, Implementor implementor) {
				RexInputRef ref = (RexInputRef) node;
				int index = ref.getIndex();
				return implementor.newColumnExpression(index);
			}
			
		});
		EXPRESSION_MAP.put(SqlKind.CAST, new ExpressionFactory() {

            @Override
            public Expression newExpression(RexNode node,
                    Implementor implementor) {
                // TODO replace with real implementation
                return toExpression(((RexCall) node).getOperands().get(0), implementor);
            }
		    
		});
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
    }

	static Expression toExpression(RexNode node, Implementor implementor) {
		ExpressionFactory eFactory = getFactory(node);
		Expression expression = eFactory.newExpression(node, implementor);
		return expression;
	}
	
	static AggregateFunction toAggregateFunction(SqlAggFunction aggFunc, List<Integer> args, Implementor implementor) {
	    FunctionFactory fFactory = getFactory(aggFunc);
	    List<Expression> exprs = Lists.newArrayListWithExpectedSize(args.size());
	    for (Integer index : args) {
	        exprs.add(implementor.newColumnExpression(index));
	    }
	    
	    return (AggregateFunction) (fFactory.newFunction(aggFunc, exprs));
	}
	
	public static interface ExpressionFactory {
		public Expression newExpression(RexNode node, Implementor implementor);
	}
	
	public static interface FunctionFactory {
	    public FunctionExpression newFunction(SqlFunction sqlFunc, List<Expression> args);
	}
}
