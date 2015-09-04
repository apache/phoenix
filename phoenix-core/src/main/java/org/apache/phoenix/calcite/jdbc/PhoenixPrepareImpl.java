package org.apache.phoenix.calcite.jdbc;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.phoenix.calcite.PhoenixSchema;
import org.apache.phoenix.calcite.parse.SqlCreateView;
import org.apache.phoenix.calcite.parser.PhoenixParserImpl;
import org.apache.phoenix.calcite.rules.PhoenixAddScanLimitRule;
import org.apache.phoenix.calcite.rules.PhoenixCompactClientSortRule;
import org.apache.phoenix.calcite.rules.PhoenixFilterScanMergeRule;
import org.apache.phoenix.calcite.rules.PhoenixInnerSortRemoveRule;
import org.apache.phoenix.calcite.rules.PhoenixJoinSingleValueAggregateMergeRule;

public class PhoenixPrepareImpl extends CalcitePrepareImpl {
    public static final ThreadLocal<String> THREAD_SQL_STRING =
        new ThreadLocal<>();

    protected final RelOptRule[] defaultConverterRules;

    public PhoenixPrepareImpl(RelOptRule[] defaultConverterRules) {
        super();
        this.defaultConverterRules = defaultConverterRules;
    }
    
    @Override
    protected SqlParser.ConfigBuilder createParserConfig() {
        return super.createParserConfig()
            .setParserFactory(PhoenixParserImpl.FACTORY);
    }

    protected SqlParser createParser(String sql,
        SqlParser.ConfigBuilder parserConfig) {
        THREAD_SQL_STRING.set(sql);
        return SqlParser.create(sql, parserConfig.build());
    }

    @Override
    protected RelOptPlanner createPlanner(
            final CalcitePrepare.Context prepareContext,
            org.apache.calcite.plan.Context externalContext,
            RelOptCostFactory costFactory) {
        RelOptPlanner planner = super.createPlanner(prepareContext, externalContext, costFactory);
        
        planner.removeRule(JoinCommuteRule.INSTANCE);
        planner.addRule(JoinCommuteRule.SWAP_OUTER);
        
        for (RelOptRule rule : this.defaultConverterRules) {
            planner.addRule(rule);
        }
        planner.addRule(PhoenixFilterScanMergeRule.INSTANCE);
        planner.addRule(PhoenixAddScanLimitRule.LIMIT_SCAN);
        planner.addRule(PhoenixAddScanLimitRule.LIMIT_SERVERPROJECT_SCAN);
        planner.addRule(PhoenixCompactClientSortRule.SORT_SERVERAGGREGATE);
        planner.addRule(PhoenixJoinSingleValueAggregateMergeRule.INSTANCE);
        planner.addRule(PhoenixInnerSortRemoveRule.INSTANCE);
        
        for (CalciteSchema subSchema : prepareContext.getRootSchema().getSubSchemaMap().values()) {
            if (subSchema.schema instanceof PhoenixSchema) {
                ((PhoenixSchema) subSchema.schema).defineIndexesAsMaterializations();
                for (CalciteSchema phoenixSubSchema : subSchema.getSubSchemaMap().values()) {
                    ((PhoenixSchema) phoenixSubSchema.schema).defineIndexesAsMaterializations();
                }
            }
        }

        return planner;
    }

    @Override
    public void executeDdl(Context context, SqlNode node) {
        switch (node.getKind()) {
        case CREATE_VIEW:
            final SqlCreateView cv = (SqlCreateView) node;
            System.out.println("Create view: " + cv.name);
            break;
        default:
            throw new AssertionError("unknown DDL type " + node.getKind() + " " + node.getClass());
        }
    }
}
