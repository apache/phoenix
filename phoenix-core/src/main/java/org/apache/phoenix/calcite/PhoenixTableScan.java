package org.apache.phoenix.calcite;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.util.List;

/**
 * Scan of a Phoenix table.
 */
public class PhoenixTableScan extends TableScan implements PhoenixRel {
    public final RexNode filter;

    protected PhoenixTableScan(RelOptCluster cluster, RelTraitSet traits, RelOptTable table, RexNode filter) {
        super(cluster, traits, table);
        this.filter = filter;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return this;
    }

    @Override
    public void register(RelOptPlanner planner) {
        RelOptRule[] rules = PhoenixRules.RULES;
        for (RelOptRule rule : rules) {
            planner.addRule(rule);
        }
        planner.addRule(PhoenixFilterScanMergeRule.INSTANCE);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .itemIf("filter", filter, filter != null);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
        RelOptCost cost = super.computeSelfCost(planner).multiplyBy(PHOENIX_FACTOR);
        if (filter != null && !filter.isAlwaysTrue()) {
            final Double selectivity = RelMetadataQuery.getSelectivity(this, filter);
            cost = cost.multiplyBy(selectivity);
        }
        return cost;
    }

    @Override
    public void implement(Implementor implementor, PhoenixConnection conn) {
        final PhoenixTable phoenixTable = table.unwrap(PhoenixTable.class);
        implementor.setContext(phoenixTable.pc, phoenixTable.getTable(), filter);
    }
}
