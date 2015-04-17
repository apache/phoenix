package org.apache.phoenix.calcite.rel;

import java.sql.SQLException;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.calcite.CalciteUtils;
import org.apache.phoenix.calcite.PhoenixTable;
import org.apache.phoenix.calcite.rules.PhoenixAddScanLimitRule;
import org.apache.phoenix.calcite.rules.PhoenixClientJoinRule;
import org.apache.phoenix.calcite.rules.PhoenixCompactClientSortRule;
import org.apache.phoenix.calcite.rules.PhoenixFilterScanMergeRule;
import org.apache.phoenix.calcite.rules.PhoenixConverterRules;
import org.apache.phoenix.calcite.rules.PhoenixServerAggregateRule;
import org.apache.phoenix.calcite.rules.PhoenixServerJoinRule;
import org.apache.phoenix.calcite.rules.PhoenixServerProjectRule;
import org.apache.phoenix.calcite.rules.PhoenixServerSortRule;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.WhereCompiler;
import org.apache.phoenix.compile.WhereOptimizer;
import org.apache.phoenix.execute.ScanPlan;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.iterate.ParallelIteratorFactory;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.schema.KeyValueSchema.KeyValueSchemaBuilder;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Scan of a Phoenix table.
 */
public class PhoenixTableScan extends TableScan implements PhoenixRel {
    public final RexNode filter;
    
    /**
     * This will not make a difference in implement(), but rather give a more accurate
     * estimate of the row count.
     */
    public final Integer statelessFetch;
    
    public static PhoenixTableScan create(RelOptCluster cluster, final RelOptTable table, 
            RexNode filter, Integer statelessFetch) {
        final RelTraitSet traits =
                cluster.traitSetOf(PhoenixRel.CONVENTION)
                .replaceIfs(RelCollationTraitDef.INSTANCE,
                        new Supplier<List<RelCollation>>() {
                    public List<RelCollation> get() {
                        if (table != null) {
                            return table.unwrap(PhoenixTable.class).getStatistic().getCollations();
                        }
                        return ImmutableList.of();
                    }
                });
        return new PhoenixTableScan(cluster, traits, table, filter, statelessFetch);
    }

    private PhoenixTableScan(RelOptCluster cluster, RelTraitSet traits, RelOptTable table, RexNode filter, Integer statelessFetch) {
        super(cluster, traits, table);
        this.filter = filter;
        this.statelessFetch = statelessFetch;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return this;
    }

    @Override
    public void register(RelOptPlanner planner) {
        RelOptRule[] rules = PhoenixConverterRules.RULES;
        for (RelOptRule rule : rules) {
            planner.addRule(rule);
        }
        planner.addRule(PhoenixFilterScanMergeRule.INSTANCE);
        planner.addRule(PhoenixAddScanLimitRule.LIMIT_SCAN);
        planner.addRule(PhoenixAddScanLimitRule.LIMIT_SERVERPROJECT_SCAN);
        planner.addRule(PhoenixServerProjectRule.PROJECT_SCAN);
        planner.addRule(PhoenixServerProjectRule.PROJECT_SERVERJOIN);
        planner.addRule(PhoenixServerJoinRule.JOIN_SCAN);
        planner.addRule(PhoenixServerJoinRule.JOIN_SERVERPROJECT_SCAN);
        planner.addRule(PhoenixServerAggregateRule.AGGREGATE_SCAN);
        planner.addRule(PhoenixServerAggregateRule.AGGREGATE_SERVERJOIN);
        planner.addRule(PhoenixServerAggregateRule.AGGREGATE_SERVERPROJECT);
        planner.addRule(PhoenixServerSortRule.SORT_SCAN);
        planner.addRule(PhoenixServerSortRule.SORT_SERVERJOIN);
        planner.addRule(PhoenixServerSortRule.SORT_SERVERPROJECT);
        planner.addRule(PhoenixCompactClientSortRule.SORT_SERVERAGGREGATE);
        planner.addRule(PhoenixClientJoinRule.INSTANCE);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .itemIf("filter", filter, filter != null)
            .itemIf("statelessFetch", statelessFetch, statelessFetch != null);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
        double rowCount = RelMetadataQuery.getRowCount(this);
        return planner.getCostFactory()
                .makeCost(rowCount, rowCount + 1, 0)
                .multiplyBy(PHOENIX_FACTOR);
    }
    
    @Override
    public double getRows() {
        double rows = super.getRows();
        if (filter != null && !filter.isAlwaysTrue()) {
            rows = rows * RelMetadataQuery.getSelectivity(this, filter);
        }        
        if (statelessFetch == null)
            return rows;
        
        return Math.min(statelessFetch, rows);
    }

    @Override
    public QueryPlan implement(Implementor implementor) {
        final PhoenixTable phoenixTable = table.unwrap(PhoenixTable.class);
        PTable pTable = phoenixTable.getTable();
        TableRef tableRef = new TableRef(CalciteUtils.createTempAlias(), pTable, HConstants.LATEST_TIMESTAMP, false);
        implementor.setTableRef(tableRef);
        try {
            PhoenixStatement stmt = new PhoenixStatement(phoenixTable.pc);
            ColumnResolver resolver = FromCompiler.getResolver(tableRef);
            StatementContext context = new StatementContext(stmt, resolver, new Scan(), new SequenceManager(stmt));
            SelectStatement select = SelectStatement.SELECT_STAR;
            if (filter != null) {
                Expression filterExpr = CalciteUtils.toExpression(filter, implementor);
                filterExpr = WhereOptimizer.pushKeyExpressionsToScan(context, select, filterExpr);
                WhereCompiler.setScanFilter(context, select, filterExpr, true, false);
            }
            projectAllColumnFamilies(context.getScan(), phoenixTable.getTable());
            if (implementor.getCurrentContext().forceProject()) {
                TupleProjector tupleProjector = createTupleProjector(implementor, phoenixTable.getTable());
                TupleProjector.serializeProjectorIntoScan(context.getScan(), tupleProjector);
                PTable projectedTable = implementor.createProjectedTable();
                implementor.setTableRef(new TableRef(projectedTable));
            }
            Integer limit = null;
            OrderBy orderBy = OrderBy.EMPTY_ORDER_BY;
            ParallelIteratorFactory iteratorFactory = null;
            return new ScanPlan(context, select, tableRef, RowProjector.EMPTY_PROJECTOR, limit, orderBy, iteratorFactory, true);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    
    private TupleProjector createTupleProjector(Implementor implementor, PTable table) {
        KeyValueSchemaBuilder builder = new KeyValueSchemaBuilder(0);
        List<Expression> exprs = Lists.<Expression> newArrayList();
        for (PColumn column : table.getColumns()) {
            if (!SchemaUtil.isPKColumn(column) || !implementor.getCurrentContext().isRetainPKColumns()) {
                Expression expr = implementor.newColumnExpression(column.getPosition());
                exprs.add(expr);
                builder.addField(expr);                
            }
        }
        
        return new TupleProjector(builder.build(), exprs.toArray(new Expression[exprs.size()]));
    }
    
    // TODO only project needed columns
    private void projectAllColumnFamilies(Scan scan, PTable table) {
        scan.getFamilyMap().clear();
        for (PColumnFamily family : table.getColumnFamilies()) {
            scan.addFamily(family.getName().getBytes());
        }
    }
}
