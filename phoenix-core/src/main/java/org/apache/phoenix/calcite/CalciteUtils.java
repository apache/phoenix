package org.apache.phoenix.calcite;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.calcite.rel.PhoenixRel.Implementor;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.ComparisonExpression;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.OrExpression;
import org.apache.phoenix.expression.function.AggregateFunction;
import org.apache.phoenix.expression.function.CountAggregateFunction;
import org.apache.phoenix.expression.function.FunctionExpression;
import org.apache.phoenix.expression.function.MaxAggregateFunction;
import org.apache.phoenix.expression.function.MinAggregateFunction;

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
            public Expression newExpression(RexNode node, Implementor implementor) {
                try {
                    return AndExpression.create(convertChildren((RexCall) node, implementor));
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            
        });
        EXPRESSION_MAP.put(SqlKind.OR, new ExpressionFactory() {

            @Override
            public Expression newExpression(RexNode node, Implementor implementor) {
                return new OrExpression(convertChildren((RexCall) node, implementor));
            }
            
        });
		EXPRESSION_MAP.put(SqlKind.EQUALS, new ExpressionFactory() {

			@Override
			public Expression newExpression(RexNode node, Implementor implementor) {
				ImmutableBytesWritable ptr = new ImmutableBytesWritable();
				try {
					return ComparisonExpression.create(CompareOp.EQUAL, convertChildren((RexCall) node, implementor), ptr);
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
			}
			
		});
        EXPRESSION_MAP.put(SqlKind.NOT_EQUALS, new ExpressionFactory() {

            @Override
            public Expression newExpression(RexNode node, Implementor implementor) {
                ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                try {
                    return ComparisonExpression.create(CompareOp.NOT_EQUAL, convertChildren((RexCall) node, implementor), ptr);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            
        });
        EXPRESSION_MAP.put(SqlKind.GREATER_THAN, new ExpressionFactory() {

            @Override
            public Expression newExpression(RexNode node, Implementor implementor) {
                ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                try {
                    return ComparisonExpression.create(CompareOp.GREATER, convertChildren((RexCall) node, implementor), ptr);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            
        });
        EXPRESSION_MAP.put(SqlKind.GREATER_THAN_OR_EQUAL, new ExpressionFactory() {

            @Override
            public Expression newExpression(RexNode node, Implementor implementor) {
                ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                try {
                    return ComparisonExpression.create(CompareOp.GREATER_OR_EQUAL, convertChildren((RexCall) node, implementor), ptr);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            
        });
        EXPRESSION_MAP.put(SqlKind.LESS_THAN, new ExpressionFactory() {

            @Override
            public Expression newExpression(RexNode node, Implementor implementor) {
                ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                try {
                    return ComparisonExpression.create(CompareOp.LESS, convertChildren((RexCall) node, implementor), ptr);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            
        });
        EXPRESSION_MAP.put(SqlKind.LESS_THAN_OR_EQUAL, new ExpressionFactory() {

            @Override
            public Expression newExpression(RexNode node, Implementor implementor) {
                ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                try {
                    return ComparisonExpression.create(CompareOp.LESS_OR_EQUAL, convertChildren((RexCall) node, implementor), ptr);
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
        // TODO Buggy. Disable for now.
        //FUNCTION_MAP.put("$SUM0", new FunctionFactory() {
        //    @Override
        //    public FunctionExpression newFunction(SqlFunction sqlFunc,
        //            List<Expression> args) {
        //        return new SumAggregateFunction(args);
        //    }
        //});
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
    
    private static List<Expression> convertChildren(RexCall call, Implementor implementor) {
        List<Expression> children = Lists.newArrayListWithExpectedSize(call.getOperands().size());
        for (RexNode op : call.getOperands()) {
            Expression child = getFactory(op).newExpression(op, implementor);
            children.add(child);
        }
        return children;
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

	public static Expression toExpression(RexNode node, Implementor implementor) {
		ExpressionFactory eFactory = getFactory(node);
		Expression expression = eFactory.newExpression(node, implementor);
		return expression;
	}
	
	public static AggregateFunction toAggregateFunction(SqlAggFunction aggFunc, List<Integer> args, Implementor implementor) {
	    FunctionFactory fFactory = getFactory(aggFunc);
	    List<Expression> exprs = Lists.newArrayListWithExpectedSize(args.size());
	    for (Integer index : args) {
	        exprs.add(implementor.newColumnExpression(index));
	    }
	    
	    return (AggregateFunction) (fFactory.newFunction(aggFunc, exprs));
	}
	
	public static Object evaluateStatelessExpression(RexNode node) {
	    try {
	        Expression expression = toExpression(node, null);
	        if (expression.isStateless() && expression.getDeterminism() == Determinism.ALWAYS) {
	            ImmutableBytesWritable ptr = new ImmutableBytesWritable();
	            expression.evaluate(null, ptr);
	            return expression.getDataType().toObject(ptr);
	        }
	    } catch (Exception e) {
	        // Expression is not stateless. do nothing.
	    }
	    
	    return null;
	}
	
	public static interface ExpressionFactory {
		public Expression newExpression(RexNode node, Implementor implementor);
	}
	
	public static interface FunctionFactory {
	    public FunctionExpression newFunction(SqlFunction sqlFunc, List<Expression> args);
	}
}
