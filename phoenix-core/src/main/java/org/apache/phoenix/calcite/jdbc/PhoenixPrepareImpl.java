package org.apache.phoenix.calcite.jdbc;

import java.util.List;

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.prepare.Prepare.Materialization;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.calcite.rel.rules.SortUnionTransposeRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.Pair;
import org.apache.phoenix.calcite.PhoenixSchema;
import org.apache.phoenix.calcite.parse.SqlCreateTable;
import org.apache.phoenix.calcite.parse.SqlCreateView;
import org.apache.phoenix.calcite.parser.PhoenixParserImpl;
import org.apache.phoenix.calcite.rel.PhoenixRel;
import org.apache.phoenix.calcite.rel.PhoenixServerProject;
import org.apache.phoenix.calcite.rel.PhoenixTemporarySort;
import org.apache.phoenix.calcite.rules.PhoenixCompactClientSortRule;
import org.apache.phoenix.calcite.rules.PhoenixFilterScanMergeRule;
import org.apache.phoenix.calcite.rules.PhoenixForwardTableScanRule;
import org.apache.phoenix.calcite.rules.PhoenixJoinSingleValueAggregateMergeRule;
import org.apache.phoenix.calcite.rules.PhoenixMergeSortUnionRule;
import org.apache.phoenix.calcite.rules.PhoenixOrderedAggregateRule;
import org.apache.phoenix.calcite.rules.PhoenixReverseTableScanRule;
import org.apache.phoenix.calcite.rules.PhoenixSortServerJoinTransposeRule;
import org.apache.phoenix.calcite.rules.PhoenixTableScanColumnRefRule;

import com.google.common.base.Function;

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
    protected RelOptCluster createCluster(RelOptPlanner planner,
            RexBuilder rexBuilder) {
        RelOptCluster cluster = super.createCluster(planner, rexBuilder);
        cluster.setMetadataProvider(PhoenixRel.METADATA_PROVIDER);
        return cluster;
    }
    
    @Override
    protected RelOptPlanner createPlanner(
            final CalcitePrepare.Context prepareContext,
            org.apache.calcite.plan.Context externalContext,
            RelOptCostFactory costFactory) {
        RelOptPlanner planner = super.createPlanner(prepareContext, externalContext, costFactory);
        
        planner.removeRule(EnumerableRules.ENUMERABLE_SEMI_JOIN_RULE);
        planner.removeRule(JoinCommuteRule.INSTANCE);
        planner.addRule(JoinCommuteRule.SWAP_OUTER);
        planner.removeRule(SortUnionTransposeRule.INSTANCE);
        planner.addRule(SortUnionTransposeRule.MATCH_NULL_FETCH);
        planner.addRule(new SortProjectTransposeRule(
                PhoenixTemporarySort.class,
                PhoenixServerProject.class,
                "PhoenixSortProjectTransposeRule"));
        
        for (RelOptRule rule : this.defaultConverterRules) {
            planner.addRule(rule);
        }
        planner.addRule(PhoenixFilterScanMergeRule.INSTANCE);
        planner.addRule(PhoenixTableScanColumnRefRule.INSTANCE);
        planner.addRule(PhoenixCompactClientSortRule.INSTANCE);
        planner.addRule(PhoenixJoinSingleValueAggregateMergeRule.INSTANCE);
        planner.addRule(PhoenixMergeSortUnionRule.INSTANCE);
        planner.addRule(PhoenixOrderedAggregateRule.INSTANCE);
        planner.addRule(PhoenixSortServerJoinTransposeRule.INSTANCE);
        planner.addRule(new PhoenixForwardTableScanRule(LogicalSort.class));
        planner.addRule(new PhoenixForwardTableScanRule(PhoenixTemporarySort.class));
        planner.addRule(new PhoenixReverseTableScanRule(LogicalSort.class));
        planner.addRule(new PhoenixReverseTableScanRule(PhoenixTemporarySort.class));
                
        if (prepareContext.config().materializationsEnabled()) {
            final CalciteSchema rootSchema = prepareContext.getRootSchema();
            Hook.TRIMMED.add(new Function<RelNode, Object>() {
                boolean called = false;
                @Override
                public Object apply(RelNode root) {
                    if (!called) {
                        called = true;
                        for (CalciteSchema schema : rootSchema.getSubSchemaMap().values()) {
                            if (schema.schema instanceof PhoenixSchema) {
                                ((PhoenixSchema) schema.schema).defineIndexesAsMaterializations();
                                for (CalciteSchema subSchema : schema.getSubSchemaMap().values()) {
                                    ((PhoenixSchema) subSchema.schema).defineIndexesAsMaterializations();
                                }
                            }
                        }
                    }
                    return null;
                }            
            });
        }
        
        Hook.PROGRAM.add(new Function<Pair<List<Materialization>, Holder<Program>>, Object>() {
			@Override
			public Object apply(
					Pair<List<Materialization>, Holder<Program>> input) {
				input.getValue().set(Programs.standard(PhoenixRel.METADATA_PROVIDER));
				return null;
			}
        });

        return planner;
    }

    @Override
    public void executeDdl(Context context, SqlNode node) {
        switch (node.getKind()) {
        case CREATE_VIEW:
            final SqlCreateView cv = (SqlCreateView) node;
            System.out.println("Create view: " + cv.name);
            break;
        case CREATE_TABLE:
            final SqlCreateTable table = (SqlCreateTable) node;
            System.out.println("Create table: " + table.tableName);
            break;
        default:
            throw new AssertionError("unknown DDL type " + node.getKind() + " " + node.getClass());
        }
    }
}
