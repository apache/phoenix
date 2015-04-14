package org.apache.phoenix.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.phoenix.calcite.PhoenixClientAggregate;
import org.apache.phoenix.calcite.PhoenixRel;
import org.apache.phoenix.calcite.PhoenixServerAggregate;
import org.apache.phoenix.calcite.PhoenixServerJoin;
import org.apache.phoenix.calcite.PhoenixServerProject;
import org.apache.phoenix.calcite.PhoenixTableScan;

public class PhoenixServerAggregateRule extends RelOptRule {
    
    public static final PhoenixServerAggregateRule AGGREGATE_SCAN = 
            new PhoenixServerAggregateRule("PhoenixServerAggregateRule:aggregate_scan", PhoenixTableScan.class);
    
    public static final PhoenixServerAggregateRule AGGREGATE_SERVERJOIN = 
            new PhoenixServerAggregateRule("PhoenixServerAggregateRule:aggregate_serverjoin", PhoenixServerJoin.class);
    
    public static final PhoenixServerAggregateRule AGGREGATE_SERVERPROJECT = 
            new PhoenixServerAggregateRule("PhoenixServerAggregateRule:aggregate_serverproject", PhoenixServerProject.class);

    public PhoenixServerAggregateRule(String description, Class<? extends PhoenixRel> clazz) {
        super(
            operand(PhoenixClientAggregate.class,
                    operand(clazz, any())),
            description);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        PhoenixClientAggregate aggregate = call.rel(0);
        PhoenixRel input = call.rel(1);
        call.transformTo(new PhoenixServerAggregate(aggregate.getCluster(),
                aggregate.getTraitSet(), input, aggregate.indicator, 
                aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList()));
    }

}
