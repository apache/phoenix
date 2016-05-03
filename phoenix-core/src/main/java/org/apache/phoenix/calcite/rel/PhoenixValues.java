package org.apache.phoenix.calcite.rel;

import static org.apache.phoenix.util.PhoenixRuntime.CONNECTIONLESS;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.calcite.CalciteUtils;
import org.apache.phoenix.calcite.TableMapping;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.execute.LiteralResultIterationPlan;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Values}
 * relational expression in Phoenix.
 */
public class PhoenixValues extends Values implements PhoenixQueryRel {
    
    private static final PhoenixConnection phoenixConnection;
    static {
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            final Connection connection =
                DriverManager.getConnection(JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + CONNECTIONLESS);
            phoenixConnection =
                connection.unwrap(PhoenixConnection.class);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static PhoenixValues create(RelOptCluster cluster, final RelDataType rowType, final ImmutableList<ImmutableList<RexLiteral>> tuples) {
        final RelMetadataQuery mq = RelMetadataQuery.instance();
        final RelTraitSet traits =
                cluster.traitSetOf(PhoenixConvention.CLIENT)
                .replaceIfs(RelCollationTraitDef.INSTANCE,
                        new Supplier<List<RelCollation>>() {
                    public List<RelCollation> get() {
                        return RelMdCollation.values(mq, rowType, tuples);
                    }
                })
                .replaceIf(RelDistributionTraitDef.INSTANCE,
                        new Supplier<RelDistribution>() {
                    public RelDistribution get() {
                        return RelMdDistribution.values(rowType, tuples);
                    }
                });
        return new PhoenixValues(cluster, rowType, tuples, traits);
    }
    
    private PhoenixValues(RelOptCluster cluster, RelDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples, RelTraitSet traits) {
        super(cluster, rowType, tuples, traits);
    }

    @Override
    public PhoenixValues copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return create(getCluster(), rowType, tuples);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(PHOENIX_FACTOR);
    }

    @Override
    public QueryPlan implement(PhoenixRelImplementor implementor) {
        List<Tuple> literalResult = Lists.newArrayList();
        Iterator<ImmutableList<RexLiteral>> iter = getTuples().iterator();
        Tuple baseTuple = new SingleKeyValueTuple(KeyValue.LOWESTKEY);
        while (iter.hasNext()) {
            ImmutableList<RexLiteral> row = iter.next();
            List<Expression> exprs = Lists.newArrayListWithExpectedSize(row.size());
            for (RexLiteral rexLiteral : row) {
                exprs.add(CalciteUtils.toExpression(rexLiteral, implementor));
            }
            TupleProjector projector = implementor.project(exprs);
            literalResult.add(projector.projectResults(baseTuple));
        }
        PTable projectedTable = implementor.getTableMapping().createProjectedTable(implementor.getCurrentContext().retainPKColumns);
        TableMapping tableMapping = new TableMapping(projectedTable);
        implementor.setTableMapping(tableMapping);
        
        try {
            PhoenixStatement stmt = new PhoenixStatement(phoenixConnection);
            ColumnResolver resolver = FromCompiler.getResolver(tableMapping.getTableRef());
            StatementContext context = new StatementContext(stmt, resolver, new Scan(), new SequenceManager(stmt));
            return new LiteralResultIterationPlan(literalResult, context, SelectStatement.SELECT_ONE, TableRef.EMPTY_TABLE_REF, RowProjector.EMPTY_PROJECTOR, null, null, OrderBy.EMPTY_ORDER_BY, null);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
