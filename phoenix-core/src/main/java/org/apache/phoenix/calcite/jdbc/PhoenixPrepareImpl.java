package org.apache.phoenix.calcite.jdbc;

import java.util.List;

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.prepare.Prepare.Materialization;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.Pair;
import org.apache.phoenix.calcite.PhoenixSchema;
import org.apache.phoenix.calcite.metadata.PhoenixRelMetadataProvider;
import org.apache.phoenix.calcite.parse.SqlCreateView;
import org.apache.phoenix.calcite.parser.PhoenixParserImpl;
import org.apache.phoenix.calcite.rules.PhoenixAddScanLimitRule;
import org.apache.phoenix.calcite.rules.PhoenixCompactClientSortRule;
import org.apache.phoenix.calcite.rules.PhoenixFilterScanMergeRule;
import org.apache.phoenix.calcite.rules.PhoenixInnerSortRemoveRule;
import org.apache.phoenix.calcite.rules.PhoenixJoinSingleValueAggregateMergeRule;

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
    protected RelOptPlanner createPlanner(
            final CalcitePrepare.Context prepareContext,
            org.apache.calcite.plan.Context externalContext,
            RelOptCostFactory costFactory) {
        RelOptPlanner planner = super.createPlanner(prepareContext, externalContext, costFactory);
        
        planner.removeRule(EnumerableRules.ENUMERABLE_SEMI_JOIN_RULE);
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
        
        if (prepareContext.config().materializationsEnabled()) {
            for (CalciteSchema subSchema : prepareContext.getRootSchema().getSubSchemaMap().values()) {
                if (subSchema.schema instanceof PhoenixSchema) {
                    ((PhoenixSchema) subSchema.schema).defineIndexesAsMaterializations();
                    for (CalciteSchema phoenixSubSchema : subSchema.getSubSchemaMap().values()) {
                        ((PhoenixSchema) phoenixSubSchema.schema).defineIndexesAsMaterializations();
                    }
                }
            }
        }
        
        Hook.PROGRAM.add(new Function<Pair<List<Materialization>, Holder<Program>>, Object>() {
			@Override
			public Object apply(
					Pair<List<Materialization>, Holder<Program>> input) {
				final Program program1 =
						new Program() {
					public RelNode run(RelOptPlanner planner, RelNode rel,
							RelTraitSet requiredOutputTraits) {
						final RelNode rootRel2 =
								rel.getTraitSet().equals(requiredOutputTraits)
								? rel
										: planner.changeTraits(rel, requiredOutputTraits);
						assert rootRel2 != null;

						planner.setRoot(rootRel2);
						final RelOptPlanner planner2 = planner.chooseDelegate();
						final RelNode rootRel3 = planner2.findBestExp();
						assert rootRel3 != null : "could not implement exp";
						return rootRel3;
					}
				};

				// Second planner pass to do physical "tweaks". This the first time that
				// EnumerableCalcRel is introduced.
				final Program program2 = Programs.hep(Programs.CALC_RULES, true, new PhoenixRelMetadataProvider());;

				Program p = Programs.sequence(program1, program2);
				input.getValue().set(p);
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
        default:
            throw new AssertionError("unknown DDL type " + node.getKind() + " " + node.getClass());
        }
    }
}
