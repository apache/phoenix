package org.apache.phoenix.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.phoenix.calcite.rel.PhoenixClientSort;
import org.apache.phoenix.calcite.rel.PhoenixRel;
import org.apache.phoenix.calcite.rel.PhoenixServerJoin;
import org.apache.phoenix.calcite.rel.PhoenixServerProject;
import org.apache.phoenix.calcite.rel.PhoenixServerSort;
import org.apache.phoenix.calcite.rel.PhoenixTableScan;

public class PhoenixServerSortRule extends RelOptRule {
    
    public static final PhoenixServerSortRule SORT_SCAN = 
            new PhoenixServerSortRule("PhoenixServerSortRule:sort_scan", PhoenixTableScan.class);
    
    public static final PhoenixServerSortRule SORT_SERVERJOIN = 
            new PhoenixServerSortRule("PhoenixServerSortRule:sort_serverjoin", PhoenixServerJoin.class);
    
    public static final PhoenixServerSortRule SORT_SERVERPROJECT = 
            new PhoenixServerSortRule("PhoenixServerSortRule:sort_serverproject", PhoenixServerProject.class);

    public PhoenixServerSortRule(String description, Class<? extends PhoenixRel> clazz) {
        super(
            operand(PhoenixClientSort.class,
                    operand(clazz, any())),
            description);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        PhoenixClientSort sort = call.rel(0);
        PhoenixRel input = call.rel(1);
        call.transformTo(PhoenixServerSort.create(
                input, sort.getCollation()));
    }

}
