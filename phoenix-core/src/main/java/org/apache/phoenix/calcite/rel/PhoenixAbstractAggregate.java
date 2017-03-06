package org.apache.phoenix.calcite.rel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.phoenix.calcite.CalciteUtils;
import org.apache.phoenix.calcite.TableMapping;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.execute.TupleProjectionPlan;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.aggregator.ClientAggregators;
import org.apache.phoenix.expression.aggregator.ServerAggregators;
import org.apache.phoenix.expression.function.SingleAggregateFunction;
import org.apache.phoenix.expression.visitor.SingleAggregateFunctionVisitor;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.RowKeyValueAccessor;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Aggregate}
 * relational expression in Phoenix.
 */
abstract public class PhoenixAbstractAggregate extends Aggregate implements PhoenixQueryRel {
    
    public static boolean isSingleValueCheckAggregate(Aggregate aggregate) {
        List<AggregateCall> aggCalls = aggregate.getAggCallList();
        if (aggCalls.size() != 1)
            return false;
        
        AggregateCall call = aggCalls.get(0);
        return call.getAggregation().getName().equals("SINGLE_VALUE");
    }
    
    protected static boolean isOrderedGroupSet(ImmutableBitSet groupSet, RelNode child) {
        if (groupSet.isEmpty()) {
            return true;
        }
        
        Set<Integer> ordinals = Sets.newHashSet(groupSet.asList());
        List<RelCollation> collations = child.getTraitSet().getTraits(RelCollationTraitDef.INSTANCE);
        for (int i = 0; i < collations.size(); i++) {
            int count = 0;
            List<RelFieldCollation> fieldCollations = collations.get(i).getFieldCollations();
            if (fieldCollations.size() < ordinals.size()) {
                continue;
            }
            for (RelFieldCollation fieldCollation : fieldCollations.subList(0, ordinals.size())) {
                if (ordinals.contains(fieldCollation.getFieldIndex())) {
                    count++;
                }
            }
            if (count == ordinals.size()) {
                return true;
            }
        }
        
        return false;
    }
    
    public final boolean isOrderedGroupBy;
    
    protected PhoenixAbstractAggregate(RelOptCluster cluster, RelTraitSet traits, RelNode child, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls);
        
        this.isOrderedGroupBy = isOrderedGroupSet(groupSet, child);
    }
    
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        if (isSingleValueCheckAggregate(this))
            return planner.getCostFactory().makeInfiniteCost();
        
        double rowCount = mq.getRowCount(this);
        double spoolSize = 0;
        if (!isOrderedGroupBy) {
            double bytesPerRow = mq.getAverageRowSize(this);
            spoolSize = rowCount * bytesPerRow * 6 /* map size */;
        }
        // Aggregates with more aggregate functions cost a bit more
        float multiplier = 1f + (float) aggCalls.size() * 0.125f;
        for (AggregateCall aggCall : aggCalls) {
          if (aggCall.getAggregation().getName().equals("SUM")) {
            // Pretend that SUM costs a little bit more than $SUM0,
            // to make things deterministic.
            multiplier += 0.0125f;
          }
        }
        return planner.getCostFactory().makeCost(rowCount * multiplier + spoolSize, 0, 0);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .itemIf("isOrdered", isOrderedGroupBy, !groupSet.isEmpty());
    }
    
    protected ImmutableIntList getColumnRefList() {
        List<Integer> columnRefList = Lists.newArrayList();
        for (ImmutableBitSet set : groupSets) {
            columnRefList.addAll(set.asList());
        }
        // TODO filterArg??
        for (AggregateCall call : aggCalls) {
            columnRefList.addAll(call.getArgList());
        }
        return ImmutableIntList.copyOf(columnRefList);
    }
    
    protected GroupBy getGroupBy(PhoenixRelImplementor implementor) {
        if (groupSets.size() > 1) {
            throw new UnsupportedOperationException();
        }
        
        List<Integer> ordinals = groupSet.asList();
        if (ordinals.isEmpty()) {
            return GroupBy.EMPTY_GROUP_BY;
        }
        
        // TODO sort group by keys. not sure if there is a way to avoid this sorting,
        //      otherwise we would have to add an extra projection.
        // TODO convert key types. can be avoided?
        List<Expression> keyExprs = Lists.newArrayListWithExpectedSize(ordinals.size());
        for (int i = 0; i < ordinals.size(); i++) {
            Expression expr = implementor.newColumnExpression(ordinals.get(i));
            keyExprs.add(expr);
        }
        
        return new GroupBy.GroupByBuilder().setIsOrderPreserving(isOrderedGroupBy).setExpressions(keyExprs).setKeyExpressions(keyExprs).build();        
    }
    
    protected List<Expression> serializeAggregators(PhoenixRelImplementor implementor, StatementContext context, boolean isEmptyGroupBy) {
        if(getGroupType() != Group.SIMPLE) throw new UnsupportedOperationException();

        List<Expression> funcs = Lists.newArrayList();
        for (AggregateCall call : aggCalls) {
            Expression func =
                    CalciteUtils.toAggregateFunction(
                            call.getAggregation(), call.getArgList(),
                            call.isDistinct(), call.getType(),
                            implementor, context.getExpressionManager());
            funcs.add(func);
        }

        final Set<SingleAggregateFunction> aggFuncSet = Sets.newHashSetWithExpectedSize(context.getExpressionManager().getExpressionCount());

        Iterator<Expression> expressions = context.getExpressionManager().getExpressions();
        while (expressions.hasNext()) {
            Expression expression = expressions.next();
            expression.accept(new SingleAggregateFunctionVisitor() {
                @Override
                public Iterator<Expression> visitEnter(SingleAggregateFunction function) {
                    aggFuncSet.add(function);
                    return Iterators.emptyIterator();
                }
            });
        }
        if (aggFuncSet.isEmpty() && isEmptyGroupBy) {
            return funcs;
        }
        List<SingleAggregateFunction> aggFuncs = new ArrayList<SingleAggregateFunction>(aggFuncSet);
        Collections.sort(aggFuncs, SingleAggregateFunction.SCHEMA_COMPARATOR);

        int minNullableIndex = getMinNullableIndex(aggFuncs, isEmptyGroupBy);
        context.getScan().setAttribute(BaseScannerRegionObserver.AGGREGATORS, ServerAggregators.serialize(aggFuncs, minNullableIndex));
        ClientAggregators clientAggregators = new ClientAggregators(aggFuncs, minNullableIndex);
        context.getAggregationManager().setAggregators(clientAggregators);

        return funcs;
    }
    
    protected static QueryPlan wrapWithProject(PhoenixRelImplementor implementor, QueryPlan plan, List<Expression> keyExpressions, List<Expression> aggFuncs) {
        List<Expression> exprs = Lists.newArrayList();
        for (int i = 0; i < keyExpressions.size(); i++) {
            Expression keyExpr = keyExpressions.get(i);
            RowKeyValueAccessor accessor = new RowKeyValueAccessor(keyExpressions, i);
            Expression expr = new RowKeyColumnExpression(keyExpr, accessor, keyExpr.getDataType());
            exprs.add(expr);
        }
        for (Expression aggFunc : aggFuncs) {
            exprs.add(aggFunc);
        }
        
        TupleProjector tupleProjector = implementor.project(exprs);
        PTable projectedTable = implementor.getTableMapping().createProjectedTable(implementor.getCurrentContext().retainPKColumns);
        implementor.setTableMapping(new TableMapping(projectedTable));
        return new TupleProjectionPlan(plan, tupleProjector, null);
    }
    
    private static int getMinNullableIndex(List<SingleAggregateFunction> aggFuncs, boolean isUngroupedAggregation) {
        int minNullableIndex = aggFuncs.size();
        for (int i = 0; i < aggFuncs.size(); i++) {
            SingleAggregateFunction aggFunc = aggFuncs.get(i);
            if (isUngroupedAggregation ? aggFunc.getAggregator().isNullable() : aggFunc.getAggregatorExpression().isNullable()) {
                minNullableIndex = i;
                break;
            }
        }
        return minNullableIndex;
    }
    
}
