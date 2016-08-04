package org.apache.phoenix.calcite.jdbc;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.apache.calcite.sql.SqlIndexExpressionNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOptionNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.NlsString;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.calcite.PhoenixSchema;
import org.apache.phoenix.calcite.parse.SqlCreateIndex;
import org.apache.phoenix.calcite.parse.SqlCreateSequence;
import org.apache.phoenix.calcite.parse.SqlCreateTable;
import org.apache.phoenix.calcite.parse.SqlDropIndex;
import org.apache.phoenix.calcite.parse.SqlDropSequence;
import org.apache.phoenix.calcite.parse.SqlDropTable;
import org.apache.phoenix.calcite.parse.SqlUpdateStatistics;
import org.apache.phoenix.calcite.parser.PhoenixParserImpl;
import org.apache.phoenix.calcite.rel.PhoenixRel;
import org.apache.phoenix.calcite.rel.PhoenixServerProject;
import org.apache.phoenix.calcite.rel.PhoenixTemporarySort;
import org.apache.phoenix.calcite.rules.PhoenixFilterScanMergeRule;
import org.apache.phoenix.calcite.rules.PhoenixForwardTableScanRule;
import org.apache.phoenix.calcite.rules.PhoenixJoinSingleValueAggregateMergeRule;
import org.apache.phoenix.calcite.rules.PhoenixMergeSortUnionRule;
import org.apache.phoenix.calcite.rules.PhoenixOrderedAggregateRule;
import org.apache.phoenix.calcite.rules.PhoenixReverseTableScanRule;
import org.apache.phoenix.calcite.rules.PhoenixSortServerJoinTransposeRule;
import org.apache.phoenix.calcite.rules.PhoenixTableScanColumnRefRule;
import org.apache.phoenix.compile.CreateIndexCompiler;
import org.apache.phoenix.compile.CreateSequenceCompiler;
import org.apache.phoenix.compile.CreateTableCompiler;
import org.apache.phoenix.compile.MutationPlan;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.jdbc.PhoenixStatement.Operation;
import org.apache.phoenix.parse.ColumnDef;
import org.apache.phoenix.parse.ColumnDefInPkConstraint;
import org.apache.phoenix.parse.ColumnName;
import org.apache.phoenix.parse.CreateIndexStatement;
import org.apache.phoenix.parse.CreateSequenceStatement;
import org.apache.phoenix.parse.CreateTableStatement;
import org.apache.phoenix.parse.DropIndexStatement;
import org.apache.phoenix.parse.DropSequenceStatement;
import org.apache.phoenix.parse.DropTableStatement;
import org.apache.phoenix.parse.IndexKeyConstraint;
import org.apache.phoenix.parse.NamedNode;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.PrimaryKeyConstraint;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.parse.UDFParseNode;
import org.apache.phoenix.parse.UpdateStatisticsStatement;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.SortOrder;

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
        
        Hook.PROGRAM.add(new Function<org.apache.calcite.util.Pair<List<Materialization>, Holder<Program>>, Object>() {
			@Override
			public Object apply(
			        org.apache.calcite.util.Pair<List<Materialization>, Holder<Program>> input) {
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
            case CREATE_TABLE:
            case CREATE_VIEW: {
                final SqlCreateTable table = (SqlCreateTable) node;
                final PTableType tableType = table.getKind() == SqlKind.CREATE_TABLE ? PTableType.TABLE : PTableType.VIEW;
                final TableName name;
                if (table.tableName.isSimple()) {
                    name = TableName.create(null, table.tableName.getSimple());
                } else {
                    name = TableName.create(table.tableName.names.get(0), table.tableName.names.get(1));
                }
                final ListMultimap<String, Pair<String, Object>> props = convertOptions(table.tableOptions);
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
                final TableName baseTableName;
                final ParseNode where;
                if (table.baseTableName == null) {
                    baseTableName = tableType == PTableType.TABLE ? null : name;
                    where = null;
                } else {
                    if (table.baseTableName.isSimple()) {
                        baseTableName = TableName.create(null, table.baseTableName.getSimple());
                    } else {
                        baseTableName = TableName.create(table.baseTableName.names.get(0), table.baseTableName.names.get(1));
                    }
                    where = convertSqlNodeToParseNode(table.whereNode);
                }
                final List<ParseNode> splitNodes = convertSplits(table.splitKeyList, nodeFactory);
                final CreateTableStatement create = nodeFactory.createTable(
                        name, props, columnDefs, pkConstraint,
                        splitNodes, tableType, table.ifNotExists.booleanValue(),
                        baseTableName, where, 0);
                try (final PhoenixStatement stmt = new PhoenixStatement(connection)) {
                    final CreateTableCompiler compiler = new CreateTableCompiler(stmt, Operation.UPSERT);
                    final MutationPlan plan = compiler.compile(create);
                    plan.execute();
                }
                break;
            }
            case CREATE_INDEX: {
                final SqlCreateIndex index = (SqlCreateIndex) node;
                final NamedNode name = NamedNode.caseSensitiveNamedNode(index.indexName.getSimple());
                final IndexType indexType = index.isLocal.booleanValue() ? IndexType.LOCAL : IndexType.GLOBAL;
                final TableName dataTableName;
                if (index.dataTableName.isSimple()) {
                    dataTableName = TableName.create(null, index.dataTableName.getSimple());
                } else {
                    dataTableName = TableName.create(index.dataTableName.names.get(0), index.dataTableName.names.get(1));
                }
                final NamedTableNode dataTable = NamedTableNode.create(dataTableName);
                final List<Pair<ParseNode, SortOrder>> indexKeys = Lists.newArrayList();
                for (SqlNode e : index.expressions) {
                    SqlIndexExpressionNode indexExpression = (SqlIndexExpressionNode) e;
                    ParseNode exprNode = convertSqlNodeToParseNode(indexExpression.expression);
                    indexKeys.add(new Pair<ParseNode, SortOrder>(exprNode, indexExpression.sortOrder));
                }
                final IndexKeyConstraint indexKeyConstraint = nodeFactory.indexKey(indexKeys);
                final List<ColumnName> includeColumns;
                if (SqlNodeList.isEmptyList(index.includeColumns)) {
                    includeColumns = null;
                } else {
                    includeColumns = Lists.newArrayList();
                    for (SqlNode e : index.includeColumns) {
                        SqlIdentifier n = (SqlIdentifier) e;
                        ColumnName columnName;
                        if (n.isSimple()) {
                            columnName = ColumnName.caseSensitiveColumnName(n.getSimple());
                        } else {
                            columnName = ColumnName.caseSensitiveColumnName(n.names.get(0), n.names.get(1));
                        }
                        includeColumns.add(columnName);
                    }
                }
                final ListMultimap<String, Pair<String, Object>> props = convertOptions(index.indexOptions);
                final List<ParseNode> splitNodes = convertSplits(index.splitKeyList, nodeFactory);
                // TODO
                final Map<String, UDFParseNode> udfParseNodes = new HashMap<String, UDFParseNode>();
                final CreateIndexStatement create = nodeFactory.createIndex(
                        name, dataTable, indexKeyConstraint, includeColumns,
                        splitNodes, props, index.ifNotExists.booleanValue(),
                        indexType, index.async.booleanValue(), 0, udfParseNodes);
                try (final PhoenixStatement stmt = new PhoenixStatement(connection)) {
                    final CreateIndexCompiler compiler = new CreateIndexCompiler(stmt, Operation.UPSERT);
                    final MutationPlan plan = compiler.compile(create);
                    plan.execute();
                }
                break;
            }
            case CREATE_SEQUENCE: {
                final SqlCreateSequence sequence = (SqlCreateSequence) node;
                final TableName name;
                if (sequence.sequenceName.isSimple()) {
                    name = TableName.create(null, sequence.sequenceName.getSimple());
                } else {
                    name = TableName.create(sequence.sequenceName.names.get(0), sequence.sequenceName.names.get(1));
                }
                final ParseNode startWith = nodeFactory.literal(sequence.startWith.intValue(true));
                final ParseNode incrementBy = nodeFactory.literal(sequence.incrementBy.intValue(true));
                final ParseNode minValue = nodeFactory.literal(sequence.minValue.intValue(true));
                final ParseNode maxValue = nodeFactory.literal(sequence.maxValue.intValue(true));
                final ParseNode cache = nodeFactory.literal(sequence.cache.intValue(true));
                final CreateSequenceStatement create = nodeFactory.createSequence(name, startWith, incrementBy, cache, minValue, maxValue, sequence.cycle.booleanValue(), sequence.ifNotExists.booleanValue(), 0);
                try (final PhoenixStatement stmt = new PhoenixStatement(connection)) {
                    final CreateSequenceCompiler compiler = new CreateSequenceCompiler(stmt, Operation.UPSERT);
                    final MutationPlan plan = compiler.compile(create);
                    plan.execute();
                }
                break;
            }
            case DROP_TABLE:
            case DROP_VIEW: {
                final SqlDropTable table = (SqlDropTable) node;
                final PTableType tableType = table.getKind() == SqlKind.DROP_TABLE ? PTableType.TABLE : PTableType.VIEW;
                final TableName name;
                if (table.tableName.isSimple()) {
                    name = TableName.create(null, table.tableName.getSimple());
                } else {
                    name = TableName.create(table.tableName.names.get(0), table.tableName.names.get(1));
                }
                final DropTableStatement drop = nodeFactory.dropTable(
                        name, tableType, table.ifExists.booleanValue(), table.cascade.booleanValue());
                MetaDataClient client = new MetaDataClient(connection);
                client.dropTable(drop);
                break;
            }
            case DROP_INDEX: {
                final SqlDropIndex index = (SqlDropIndex) node;
                final NamedNode name = NamedNode.caseSensitiveNamedNode(index.indexName.getSimple());
                final TableName dataTableName;
                if (index.dataTableName.isSimple()) {
                    dataTableName = TableName.create(null, index.dataTableName.getSimple());
                } else {
                    dataTableName = TableName.create(index.dataTableName.names.get(0), index.dataTableName.names.get(1));
                }
                final DropIndexStatement drop = nodeFactory.dropIndex(name, dataTableName, index.ifExists.booleanValue());
                MetaDataClient client = new MetaDataClient(connection);
                client.dropIndex(drop);
                break;                
            }
            case DROP_SEQUENCE: {
                final SqlDropSequence sequence = (SqlDropSequence) node;
                final TableName name;
                if (sequence.sequenceName.isSimple()) {
                    name = TableName.create(null, sequence.sequenceName.getSimple());
                } else {
                    name = TableName.create(sequence.sequenceName.names.get(0), sequence.sequenceName.names.get(1));
                }
                final DropSequenceStatement drop = nodeFactory.dropSequence(name, sequence.ifExists.booleanValue(), 0);
                MetaDataClient client = new MetaDataClient(connection);
                client.dropSequence(drop);
                break;                
            }
            case OTHER_DDL: {
                if (node instanceof SqlUpdateStatistics) {
                    SqlUpdateStatistics updateStatsNode = (SqlUpdateStatistics) node;
                    final TableName name;
                    if (updateStatsNode.tableName.isSimple()) {
                        name = TableName.create(null, updateStatsNode.tableName.getSimple());
                    } else {
                        name = TableName.create(updateStatsNode.tableName.names.get(0), updateStatsNode.tableName.names.get(1));
                    }
                    final NamedTableNode table = NamedTableNode.create(name);
                    final Map<String, Object> props = new HashMap<String, Object>();
                    for (SqlNode optionNode : updateStatsNode.options) {
                        SqlOptionNode option = (SqlOptionNode) optionNode;
                        props.put(option.propertyName, option.value);
                    }
                    final UpdateStatisticsStatement updateStatsStmt = nodeFactory.updateStatistics(table, updateStatsNode.scope, props);
                    MetaDataClient client = new MetaDataClient(connection);
                    client.updateStatistics(updateStatsStmt);                    
                } else {
                    throw new AssertionError("unknown DDL node " + node.getClass());                    
                }
                break;
            }
            default:
                throw new AssertionError("unknown DDL type " + node.getKind() + " " + node.getClass());
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static ParseNode convertSqlNodeToParseNode(SqlNode sqlNode) throws SQLException {
        if (sqlNode == null) {
            return null;
        }

        String sql = THREAD_SQL_STRING.get();
        SqlParserPos pos = sqlNode.getParserPosition();
        int start = SqlParserUtil.lineColToIndex(sql, pos.getLineNum(), pos.getColumnNum());
        int end = SqlParserUtil.lineColToIndex(sql, pos.getEndLineNum(), pos.getEndColumnNum());
        String sqlString = sql.substring(start, end + 1);
        return new SQLParser(sqlString).parseExpression();
    }

    private static ListMultimap<String, Pair<String, Object>> convertOptions(SqlNodeList options) {
        final ListMultimap<String, Pair<String, Object>> props;
        if (SqlNodeList.isEmptyList(options)) {
            props = null;
        } else {
            props = ArrayListMultimap.<String, Pair<String, Object>>create();
            for (SqlNode optionNode : options) {
                SqlOptionNode option = (SqlOptionNode) optionNode;
                props.put(option.familyName, new Pair<String, Object>(option.propertyName, option.value));
            }
        }

        return props;
    }

    private static List<ParseNode> convertSplits(SqlNodeList splitKeyList, ParseNodeFactory nodeFactory) {
        final List<ParseNode> splits;
        if (SqlNodeList.isEmptyList(splitKeyList)) {
            splits = null;
        } else {
            splits = Lists.newArrayList();
            for (SqlNode splitKey : splitKeyList) {
                final SqlLiteral key = (SqlLiteral) splitKey;
                splits.add(nodeFactory.literal(((NlsString) key.getValue()).toString()));
            }
        }

        return splits;
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
