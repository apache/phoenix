package org.apache.phoenix.calcite;

import java.lang.reflect.Type;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.runtime.Hook.Closeable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlColumnDefInPkConstraintNode;
import org.apache.calcite.sql.SqlColumnDefNode;
import org.apache.calcite.sql.SqlFunctionArguementNode;
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
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.NlsString;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.calcite.parse.SqlAlterIndex;
import org.apache.phoenix.calcite.parse.SqlAlterTable;
import org.apache.phoenix.calcite.parse.SqlCreateFunction;
import org.apache.phoenix.calcite.parse.SqlCreateIndex;
import org.apache.phoenix.calcite.parse.SqlCreateSchema;
import org.apache.phoenix.calcite.parse.SqlCreateSequence;
import org.apache.phoenix.calcite.parse.SqlCreateTable;
import org.apache.phoenix.calcite.parse.SqlDeleteJarNode;
import org.apache.phoenix.calcite.parse.SqlDropFunction;
import org.apache.phoenix.calcite.parse.SqlDropIndex;
import org.apache.phoenix.calcite.parse.SqlDropSchema;
import org.apache.phoenix.calcite.parse.SqlDropSequence;
import org.apache.phoenix.calcite.parse.SqlDropTable;
import org.apache.phoenix.calcite.parse.SqlUpdateStatistics;
import org.apache.phoenix.calcite.parse.SqlUploadJarsNode;
import org.apache.phoenix.calcite.parse.SqlUseSchema;
import org.apache.phoenix.calcite.parser.PhoenixParserImpl;
import org.apache.phoenix.calcite.rel.PhoenixRel;
import org.apache.phoenix.calcite.rules.PhoenixConverterRules.PhoenixToEnumerableConverterRule;
import org.apache.phoenix.compile.BaseMutationPlan;
import org.apache.phoenix.compile.CreateIndexCompiler;
import org.apache.phoenix.compile.CreateSequenceCompiler;
import org.apache.phoenix.compile.CreateTableCompiler;
import org.apache.phoenix.compile.MutationPlan;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.jdbc.PhoenixStatement.Operation;
import org.apache.phoenix.parse.AddColumnStatement;
import org.apache.phoenix.parse.AlterIndexStatement;
import org.apache.phoenix.parse.ColumnDef;
import org.apache.phoenix.parse.ColumnDefInPkConstraint;
import org.apache.phoenix.parse.ColumnName;
import org.apache.phoenix.parse.CreateFunctionStatement;
import org.apache.phoenix.parse.CreateIndexStatement;
import org.apache.phoenix.parse.CreateSchemaStatement;
import org.apache.phoenix.parse.CreateSequenceStatement;
import org.apache.phoenix.parse.CreateTableStatement;
import org.apache.phoenix.parse.DropColumnStatement;
import org.apache.phoenix.parse.DropFunctionStatement;
import org.apache.phoenix.parse.DropIndexStatement;
import org.apache.phoenix.parse.DropSchemaStatement;
import org.apache.phoenix.parse.DropSequenceStatement;
import org.apache.phoenix.parse.DropTableStatement;
import org.apache.phoenix.parse.IndexKeyConstraint;
import org.apache.phoenix.parse.LiteralParseNode;
import org.apache.phoenix.parse.NamedNode;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.PFunction;
import org.apache.phoenix.parse.PFunction.FunctionArgument;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.PrimaryKeyConstraint;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.parse.UDFParseNode;
import org.apache.phoenix.parse.UpdateStatisticsStatement;
import org.apache.phoenix.parse.UseSchemaStatement;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.Sequence;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.util.SchemaUtil;

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
        for (RelOptRule rule : PhoenixPrograms.EXCLUDED_VOLCANO_RULES) {
            planner.removeRule(rule);
        }
        for (RelOptRule rule : ENUMERABLE_RULES) {
            planner.removeRule(rule);
        }

        final PhoenixConnection pc =
                getPhoenixConnection(prepareContext.getRootSchema().plus());
        try {
            final StatementContext context =
                    new StatementContext((PhoenixStatement) pc.createStatement());
            ConverterRule[] rules = PhoenixToEnumerableConverterRule
                    .createPhoenixToEnumerableConverterRules(context);
            for (ConverterRule rule : rules) {
                planner.addRule(rule);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        for (RelOptRule rule : this.defaultConverterRules) {
            planner.addRule(rule);
        }
        for (RelOptRule rule : PhoenixPrograms.ADDITIONAL_VOLCANO_RULES) {
            planner.addRule(rule);
        }

        return planner;
    }

    public <T> CalciteSignature<T> prepareQueryable(
            Context context,
            Queryable<T> queryable) {
        List<Closeable> hooks = addHooks(
                context.getRootSchema(),
                context.config().materializationsEnabled(),
                context.config().forceDecorrelate());
        try {
            return super.prepareQueryable(context, queryable);
        } finally {
            for (Closeable hook : hooks) {
                hook.close();
            }
        }
    }

    public <T> CalciteSignature<T> prepareSql(
            Context context,
            Query<T> query,
            Type elementType,
            long maxRowCount) {
        List<Closeable> hooks = addHooks(
                context.getRootSchema(),
                context.config().materializationsEnabled(),
                context.config().forceDecorrelate());
        try {
            return super.prepareSql(context, query, elementType, maxRowCount);
        } finally {
            for (Closeable hook : hooks) {
                hook.close();
            }
        }
    }
    
    private List<Closeable> addHooks(final CalciteSchema rootSchema,
            boolean materializationEnabled, final boolean forceDecorrelate) {
        final List<Closeable> hooks = Lists.newArrayList();

        hooks.add(Hook.PARSE_TREE.add(new Function<Object[], Object>() {
            @Override
            public Object apply(Object[] input) {
                for (CalciteSchema schema : rootSchema.getSubSchemaMap().values()) {
                    if (schema.schema instanceof PhoenixSchema) {
                        ((PhoenixSchema) schema.schema).clear();
                        for (CalciteSchema subSchema : schema.getSubSchemaMap().values()) {
                            ((PhoenixSchema) subSchema.schema).clear();
                        }
                    }
                }
                return null;
            }
        }));

        hooks.add(Hook.TRIMMED.add(new Function<RelNode, Object>() {
            @Override
            public Object apply(RelNode root) {
                for (CalciteSchema schema : rootSchema.getSubSchemaMap().values()) {
                    if (schema.schema instanceof PhoenixSchema) {
                        ((PhoenixSchema) schema.schema).defineIndexesAsMaterializations();
                        for (CalciteSchema subSchema : schema.getSubSchemaMap().values()) {
                            ((PhoenixSchema) subSchema.schema).defineIndexesAsMaterializations();
                        }
                    }
                }
                return null;
            }
        }));

        hooks.add(Hook.PROGRAM.add(new Function<Holder<Program>, Object>() {
            @Override
            public Object apply(Holder<Program> input) {
                input.set(
                        PhoenixPrograms.standard(
                                PhoenixRel.METADATA_PROVIDER,
                                forceDecorrelate));
                return null;
            }
        }));
        
        return hooks;
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
                final List<ColumnDef> columnDefs = getColumnDefs(table.columnDefs);
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
                        baseTableName, where, 0, table.immutable.booleanValue() ? true : null);
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
                final ParseNode startWith = sequence.startWith == null ? null : nodeFactory.literal(sequence.startWith.intValue(true));
                final ParseNode incrementBy = sequence.incrementBy == null ? null : nodeFactory.literal(sequence.incrementBy.intValue(true));
                final ParseNode minValue = sequence.minValue == null ? null : nodeFactory.literal(sequence.minValue.intValue(true));
                final ParseNode maxValue = sequence.maxValue == null ? null : nodeFactory.literal(sequence.maxValue.intValue(true));
                final ParseNode cache = sequence.cache == null ? null : nodeFactory.literal(sequence.cache.intValue(true));
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
            case ALTER_TABLE: 
            case ALTER_VIEW: {
                final SqlAlterTable alterTable = (SqlAlterTable) node;
                final TableName name;
                if (alterTable.tableName.isSimple()) {
                    name = TableName.create(null, alterTable.tableName.getSimple());
                } else {
                    name = TableName.create(alterTable.tableName.names.get(0), alterTable.tableName.names.get(1));
                }
                final NamedTableNode namedTable = NamedTableNode.create(name);
                if(alterTable.newColumnDefs != null || alterTable.tableOptions != null) {
                    final List<ColumnDef> columnDefs = getColumnDefs(alterTable.newColumnDefs);
                    boolean ifNotExists = false;
                    if(alterTable.ifNotExists != null) {
                        ifNotExists = alterTable.ifNotExists.booleanValue();
                    }
                    final ListMultimap<String, Pair<String, Object>> props = convertOptions(alterTable.tableOptions);
                    AddColumnStatement addColumn =
                            nodeFactory
                                    .addColumn(
                                        namedTable,
                                        alterTable.isView.booleanValue() ? PTableType.VIEW
                                                : (QueryConstants.SYSTEM_SCHEMA_NAME.equals(name
                                                        .getSchemaName()) ? PTableType.SYSTEM
                                                        : PTableType.TABLE), columnDefs,
                                        ifNotExists, props);
                    MetaDataClient client = new MetaDataClient(connection);
                    client.addColumn(addColumn);
                } else {
                    final List<ColumnName> columnNames = Lists.newArrayList();
                    for (SqlNode e : alterTable.columnNames) {
                        SqlIdentifier n = (SqlIdentifier) e;
                        ColumnName columnName;
                        if (n.isSimple()) {
                            columnName = ColumnName.caseSensitiveColumnName(n.getSimple());
                        } else {
                            columnName = ColumnName.caseSensitiveColumnName(n.names.get(0), n.names.get(1));
                        }
                        columnNames.add(columnName);
                    }
                    boolean ifExists = false;
                    if(alterTable.ifExists != null) {
                        ifExists = alterTable.ifExists.booleanValue();
                    }
                    DropColumnStatement dropColumn =
                            nodeFactory.dropColumn(
                                namedTable,
                                alterTable.isView.booleanValue() ? PTableType.VIEW
                                        : (QueryConstants.SYSTEM_SCHEMA_NAME.equals(name
                                                .getSchemaName()) ? PTableType.SYSTEM
                                                : PTableType.TABLE), columnNames, ifExists);
                    MetaDataClient client = new MetaDataClient(connection);
                    client.dropColumn(dropColumn);
                }
                break;
            }
            case ALTER_INDEX: {
                final SqlAlterIndex index = (SqlAlterIndex) node;
                NamedTableNode namedTable =
                        nodeFactory.namedTable(null, TableName.create(!index.dataTableName
                                .isSimple() ? index.dataTableName.names.get(0) : null,
                            index.indexName.getSimple()));
                final String dataTableName;
                if (index.dataTableName.isSimple()) {
                    dataTableName = index.dataTableName.getSimple();
                } else {
                    dataTableName = index.dataTableName.names.get(1);
                }
                String indexState = index.indexState.names.get(0);
                PIndexState state = null;
                try {
                    state = PIndexState.valueOf(indexState.toUpperCase());
                } catch(IllegalArgumentException e) {
                    throw new SQLException(indexState+" is not a valid index state.");
                }
                boolean ifExists = index.ifExists.booleanValue();
                boolean async = index.async.booleanValue();
                AlterIndexStatement alterIndex = new AlterIndexStatement(namedTable, dataTableName, ifExists, state, async);
                MetaDataClient client = new MetaDataClient(connection);
                client.alterIndex(alterIndex);
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
                } else if (node instanceof SqlCreateFunction) {
                    SqlCreateFunction createFunctionNode = (SqlCreateFunction) node;
                    short i = 0;
                    List<FunctionArgument> functionArguements =
                            new ArrayList<FunctionArgument>(
                                    createFunctionNode.functionArguements.size());
                    for (SqlNode functionArguement : createFunctionNode.functionArguements) {
                        LiteralExpression dvExpression = null;
                        LiteralExpression minValueExpression = null;
                        LiteralExpression maxValueExpression = null;
                        SqlFunctionArguementNode funArgNode =
                                (SqlFunctionArguementNode) functionArguement;
                        if (funArgNode.defaultValue != null) {
                            LiteralParseNode dv =
                                    (LiteralParseNode) convertSqlNodeToParseNode(funArgNode.defaultValue);
                            dvExpression = LiteralExpression.newConstant(dv.getValue());
                        }
                        if (funArgNode.minValue != null) {
                            LiteralParseNode minValue =
                                    (LiteralParseNode) convertSqlNodeToParseNode(funArgNode.minValue);
                            minValueExpression = LiteralExpression.newConstant(minValue.getValue());
                        }
                        if (funArgNode.maxValue != null) {
                            LiteralParseNode maxValue =
                                    (LiteralParseNode) convertSqlNodeToParseNode(funArgNode.maxValue);
                            maxValueExpression = LiteralExpression.newConstant(maxValue.getValue());
                        }
                        functionArguements.add(new PFunction.FunctionArgument(
                                funArgNode.typeNode.typeName, funArgNode.typeNode.isArray,
                                funArgNode.isConstant, dvExpression, minValueExpression,
                                maxValueExpression, i));
                        i++;
                    }

                    final SqlLiteral className = (SqlLiteral) createFunctionNode.className;
                    String quotedClassNameStr = ((NlsString) className.getValue()).toString();
                    String classNameStr = quotedClassNameStr.substring(1, quotedClassNameStr.length() - 1);
                    String jarPathStr = null;
                    if (createFunctionNode.jarPath != null) {
                        final SqlLiteral jarPath = (SqlLiteral) createFunctionNode.jarPath;
                        String quotedJarPathStr = ((NlsString) jarPath.getValue()).toString();
                        jarPathStr = quotedJarPathStr.substring(1, quotedJarPathStr.length() - 1);
                    }
                    PFunction function =
                            new PFunction(createFunctionNode.functionName.getSimple(),
                                    functionArguements, createFunctionNode.returnType.getSimple(),
                                    classNameStr, jarPathStr);
                    CreateFunctionStatement createFunction =
                            nodeFactory.createFunction(function,
                                createFunctionNode.tempFunction.booleanValue(),
                                createFunctionNode.replace.booleanValue());
                    MetaDataClient client = new MetaDataClient(connection);
                    client.createFunction(createFunction);
                } else if (node instanceof SqlDropFunction) {
                    SqlDropFunction dropFunctionNode = (SqlDropFunction) node;
                    DropFunctionStatement dropFunctionStmt =
                            new DropFunctionStatement(dropFunctionNode.functionName.getSimple(),
                                    dropFunctionNode.ifExists.booleanValue());
                    MetaDataClient client = new MetaDataClient(connection);
                    client.dropFunction(dropFunctionStmt);
                } else if (node instanceof SqlUploadJarsNode) {
                    PhoenixStatement phoenixStatement = new PhoenixStatement(connection);
                    List<SqlNode> operandList = ((SqlUploadJarsNode) node).getOperandList();
                    List<LiteralParseNode> jarsPaths = new ArrayList<LiteralParseNode>();
                    for (SqlNode jarPath : operandList) {
                        jarsPaths.add((LiteralParseNode) convertSqlNodeToParseNode(jarPath));
                    }
                    MutationPlan compilePlan =
                            new PhoenixStatement.ExecutableAddJarsStatement(jarsPaths).compilePlan(phoenixStatement,
                                Sequence.ValueOp.VALIDATE_SEQUENCE);
                    ((BaseMutationPlan) compilePlan).execute();
                } else if (node instanceof SqlDeleteJarNode) {
                    PhoenixStatement phoenixStatement = new PhoenixStatement(connection);
                    List<SqlNode> operandList = ((SqlDeleteJarNode) node).getOperandList();
                    LiteralParseNode jarPath =
                            (LiteralParseNode) convertSqlNodeToParseNode(operandList.get(0));
                    MutationPlan compilePlan =
                            new PhoenixStatement.ExecutableDeleteJarStatement(jarPath).compilePlan(phoenixStatement,
                                Sequence.ValueOp.VALIDATE_SEQUENCE);
                    ((BaseMutationPlan) compilePlan).execute();
                } else if( node instanceof SqlCreateSchema) {
                    SqlCreateSchema createSchemaNode = (SqlCreateSchema) node;
                    CreateSchemaStatement createSchema =
                            nodeFactory.createSchema(createSchemaNode.schemaName.getSimple(),
                                createSchemaNode.ifNotExists.booleanValue());
                    MetaDataClient client = new MetaDataClient(connection);
                    client.createSchema(createSchema);
                } else if( node instanceof SqlDropSchema) {
                    SqlDropSchema dropSchemaNode = (SqlDropSchema) node;
                    DropSchemaStatement dropSchema =
                            nodeFactory.dropSchema(dropSchemaNode.schemaName.getSimple(),
                                dropSchemaNode.ifExists.booleanValue(),
                                dropSchemaNode.cascade.booleanValue());
                    MetaDataClient client = new MetaDataClient(connection);
                    client.dropSchema(dropSchema);
                } else if( node instanceof SqlUseSchema) {
                    SqlUseSchema useSchemaNode = (SqlUseSchema) node;
                    UseSchemaStatement useSchema =
                            nodeFactory.useSchema(useSchemaNode.schemaName.getSimple().equals(
                                SchemaUtil.SCHEMA_FOR_DEFAULT_NAMESPACE) ? null
                                    : useSchemaNode.schemaName.getSimple());
                    MetaDataClient client = new MetaDataClient(connection);
                    client.useSchema(useSchema);
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

    public List<ColumnDef> getColumnDefs(SqlNodeList sqlColumnDefs)
            throws SQLException {
        if(sqlColumnDefs == null) {
            return Collections.<ColumnDef>emptyList();
        }
        List<ColumnDef> columnDefs = new ArrayList<ColumnDef>(sqlColumnDefs.size());
        for(SqlNode columnDef : sqlColumnDefs) {
            SqlColumnDefNode columnDefNode = (SqlColumnDefNode) columnDef;
            if(columnDefNode.defaultValueExp!=null) {
                 ParseNode defaultValueNode= convertSqlNodeToParseNode(columnDefNode.defaultValueExp);
                 columnDefs.add(new ColumnDef(columnDefNode.columnDef, defaultValueNode.toString()));
            } else {
                columnDefs.add(columnDefNode.columnDef);
            }
        }
        return columnDefs;
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
                Object value = key.getValue();
                if(key.getValue() instanceof NlsString) {
                    String quotedValue = ((NlsString) key.getValue()).toString();
                    value = quotedValue.substring(1, quotedValue.length() - 1);
                }
                splits.add(nodeFactory.literal(value));
            }
        }

        return splits;
    }
    
    public static PhoenixConnection getPhoenixConnection(SchemaPlus rootSchema) {
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
