package org.apache.phoenix.calcite.jdbc;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.phoenix.calcite.rules.PhoenixAddScanLimitRule;
import org.apache.phoenix.calcite.rules.PhoenixClientJoinRule;
import org.apache.phoenix.calcite.rules.PhoenixCompactClientSortRule;
import org.apache.phoenix.calcite.rules.PhoenixConverterRules;
import org.apache.phoenix.calcite.rules.PhoenixFilterScanMergeRule;
import org.apache.phoenix.calcite.rules.PhoenixJoinSingleValueAggregateMergeRule;
import org.apache.phoenix.calcite.rules.PhoenixServerAggregateRule;
import org.apache.phoenix.calcite.rules.PhoenixServerJoinRule;
import org.apache.phoenix.calcite.rules.PhoenixServerProjectRule;
import org.apache.phoenix.calcite.rules.PhoenixServerSortRule;

public class PhoenixPrepareImpl extends CalcitePrepareImpl {

    public PhoenixPrepareImpl() {
        super();
    }
    
    @Override
    protected RelOptPlanner createPlanner(
            final CalcitePrepare.Context prepareContext,
            org.apache.calcite.plan.Context externalContext,
            RelOptCostFactory costFactory) {
        RelOptPlanner planner = super.createPlanner(prepareContext, externalContext, costFactory);
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
        planner.addRule(PhoenixJoinSingleValueAggregateMergeRule.INSTANCE);

        return planner;
    }
}
