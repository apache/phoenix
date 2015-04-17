package org.apache.phoenix.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.phoenix.calcite.rel.PhoenixClientAggregate;
import org.apache.phoenix.calcite.rel.PhoenixRel;
import org.apache.phoenix.calcite.rel.PhoenixServerAggregate;
import org.apache.phoenix.calcite.rel.PhoenixServerJoin;
import org.apache.phoenix.calcite.rel.PhoenixServerProject;
import org.apache.phoenix.calcite.rel.PhoenixTableScan;

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
        call.transformTo(PhoenixServerAggregate.create(input, aggregate.indicator, 
                aggregate.getGroupSet(), aggregate.getGroupSets(), 
                aggregate.getAggCallList()));
    }

}
