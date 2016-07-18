package org.apache.phoenix.calcite.jdbc;

import java.sql.SQLException;
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
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlColumnDefInPkConstraintNode;
import org.apache.calcite.sql.SqlColumnDefNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlTableOptionNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.NlsString;
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
import org.apache.phoenix.compile.CreateTableCompiler;
import org.apache.phoenix.compile.MutationPlan;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.jdbc.PhoenixStatement.Operation;
import org.apache.phoenix.parse.ColumnDef;
import org.apache.phoenix.parse.ColumnDefInPkConstraint;
import org.apache.phoenix.parse.CreateTableStatement;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.PrimaryKeyConstraint;
import org.apache.phoenix.schema.PTableType;

import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;

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
        try {
            final ParseNodeFactory nodeFactory = new ParseNodeFactory();
            final PhoenixConnection connection = getPhoenixConnection(context.getRootSchema().plus());
            switch (node.getKind()) {
            case CREATE_VIEW:
                final SqlCreateView cv = (SqlCreateView) node;
                System.out.println("Create view: " + cv.name);
                break;
            case CREATE_TABLE:
                final SqlCreateTable table = (SqlCreateTable) node;
                final SqlIdentifier tableIdentifier = table.tableName;
                final String schemaName = tableIdentifier.isSimple() ? null : tableIdentifier.skipLast(1).toString();
                final String tableName = tableIdentifier.names.get(tableIdentifier.names.size() - 1);
                final ListMultimap<String, org.apache.hadoop.hbase.util.Pair<String, Object>> props;
                if (SqlNodeList.isEmptyList(table.tableOptions)) {
                    props = null;
                } else {
                    props = ArrayListMultimap.<String, org.apache.hadoop.hbase.util.Pair<String, Object>>create();
                    for (SqlNode tableOption : table.tableOptions) {
                        SqlTableOptionNode option = (SqlTableOptionNode) tableOption;
                        props.put(option.familyName, new org.apache.hadoop.hbase.util.Pair<String, Object>(option.propertyName, option.value));
                    }
                }
                final List<ColumnDef> columnDefs = Lists.newArrayList();
                for (SqlNode columnDef : table.columnDefs) {
                    columnDefs.add(((SqlColumnDefNode) columnDef).columnDef);
                }
                final PrimaryKeyConstraint pkConstraint;
                if (table.pkConstraint == null) {
                    pkConstraint = null;
                } else {
                    final List<ColumnDefInPkConstraint> pkColumns = Lists.newArrayList();
                    for (SqlNode pkColumn : table.pkConstraintColumnDefs) {
                        pkColumns.add(((SqlColumnDefInPkConstraintNode) pkColumn).pkConstraint);
                    }
                    pkConstraint = nodeFactory.primaryKey(table.pkConstraint.getSimple(), pkColumns);
                }
                final List<ParseNode> splitNodes;
                if (SqlNodeList.isEmptyList(table.splitKeyList)) {
                    splitNodes = null;
                } else {
                    splitNodes = Lists.newArrayList();
                    for (SqlNode splitKey : table.splitKeyList) {
                        final SqlLiteral key = (SqlLiteral) splitKey;
                        splitNodes.add(nodeFactory.literal(((NlsString) key.getValue()).toString()));
                    }
                }
                final CreateTableStatement create = nodeFactory.createTable(
                        nodeFactory.table(schemaName, tableName),
                        props, columnDefs, pkConstraint,
                        splitNodes, PTableType.TABLE, table.ifNotExists.booleanValue(),
                        null, null, 0);
                try (final PhoenixStatement stmt = new PhoenixStatement(connection)) {
                    final CreateTableCompiler compiler = new CreateTableCompiler(stmt, Operation.UPSERT);
                    final MutationPlan plan = compiler.compile(create);
                    plan.execute();
                }
                break;
            default:
                throw new AssertionError("unknown DDL type " + node.getKind() + " " + node.getClass());
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    private static PhoenixConnection getPhoenixConnection(SchemaPlus rootSchema) {
        for (String subSchemaName : rootSchema.getSubSchemaNames()) {               
            try {
                PhoenixSchema phoenixSchema = rootSchema
                        .getSubSchema(subSchemaName).unwrap(PhoenixSchema.class);
                return phoenixSchema.pc;
            } catch (ClassCastException e) {
            }
        }

        throw new RuntimeException("Phoenix schema not found.");
    }
}
