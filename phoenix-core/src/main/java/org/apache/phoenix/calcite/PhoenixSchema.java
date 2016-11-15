package org.apache.phoenix.calcite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.materialize.MaterializationService;
import org.apache.calcite.schema.*;
import org.apache.calcite.schema.impl.TableFunctionImpl;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.sql.ListJarsTable;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.expression.function.UDFExpression;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.ColumnDef;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.PFunction;
import org.apache.phoenix.parse.SequenceValueParseNode;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunctionInfo;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunctionArgInfo;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.schema.PTable.ViewType;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.Sequence;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDataTypeFactory;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.SchemaUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Implementation of Calcite's {@link Schema} SPI for Phoenix.
 * 
 * TODO
 * 1) change this to non-caching mode??
 * 2) how to deal with define indexes and views since they require a CalciteSchema
 *    instance??
 *
 */
public class PhoenixSchema implements Schema {
    public static final Factory FACTORY = new Factory();

    public final PhoenixConnection pc;
    
    protected final String name;
    protected final String schemaName;
    protected final SchemaPlus parentSchema;
    protected final MetaDataClient client;
    protected final SequenceManager sequenceManager;
    
    protected final Map<String, Schema> subSchemas;
    protected final Map<String, Table> tables;
    protected final Map<String, Function> views;
    protected final Set<TableRef> viewTables;
    protected final UDFExpression exp = new UDFExpression();
    private final static Function listJarsFunction = TableFunctionImpl
            .create(ListJarsTable.LIST_JARS_TABLE_METHOD);
    public final static Map<String, Collection<Function>> builtinFunctions = Maps.newHashMap();

    
    protected PhoenixSchema(String name, String schemaName,
            SchemaPlus parentSchema, PhoenixConnection pc) {
        this.name = name;
        this.schemaName = schemaName;
        this.parentSchema = parentSchema;
        this.pc = pc;
        this.client = new MetaDataClient(pc);
        this.subSchemas = Maps.newHashMap();
        this.tables = Maps.newHashMap();
        this.views = Maps.newHashMap();
        this.views.put("ListJars", listJarsFunction);
        this.viewTables = Sets.newHashSet();
        try {
            PhoenixStatement stmt = (PhoenixStatement) pc.createStatement();
            this.sequenceManager = new SequenceManager(stmt);
        } catch (SQLException e){
            throw new RuntimeException(e);
        }
        registerBuiltinFunctions();

    }

    private static Schema create(SchemaPlus parentSchema,
            String name, Map<String, Object> operand) {
        String url = (String) operand.get("url");
        final Properties properties = new Properties();
        for (Map.Entry<String, Object> entry : operand.entrySet()) {
            properties.setProperty(entry.getKey(), String.valueOf(entry.getValue()));
        }
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            final Connection connection =
                DriverManager.getConnection(url, properties);
            final PhoenixConnection phoenixConnection =
                connection.unwrap(PhoenixConnection.class);
            return new PhoenixSchema(name, null, parentSchema, phoenixConnection);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void registerBuiltinFunctions(){
        if(!builtinFunctions.isEmpty()) {
            return;
        }
        Collection<BuiltInFunctionInfo>  infoCollection = ParseNodeFactory.getSingleEntryFunctionMap();
        for (BuiltInFunctionInfo info : infoCollection) {
            if(!CalciteUtils.TRANSLATED_BUILT_IN_FUNCTIONS.contains(info.getName())) {
                builtinFunctions.put(info.getName(),
                        (List<Function>) (Object) PhoenixScalarFunction.createBuiltinFunctions(info));
            }
        }

    }

    public static List<List<PFunction.FunctionArgument>> overloadArguments(BuiltInFunctionArgInfo[] args){
        List<List<PFunction.FunctionArgument>> overloadedArgs = Lists.newArrayList();
        int solutions = 1;
        for(int i = 0; i < args.length; solutions *= args[i].getAllowedTypes().length, i++);
        for(int i = 0; i < solutions; i++) {
            int j = 1;
            short k = 0;
            overloadedArgs.add(new ArrayList<PFunction.FunctionArgument>());
            for(BuiltInFunctionArgInfo arg : args) {
                Class<? extends PDataType>[] temp = arg.getAllowedTypes();
                PDataType dataType = PDataTypeFactory.getInstance().instanceFromClass(temp[(i/j)%temp.length]);
                overloadedArgs.get(i).add( new PFunction.FunctionArgument(
                        dataType.toString(),
                        dataType.isArrayType(),
                        arg.isConstant(),
                        arg.getDefaultValue(),
                        arg.getMinValue(),
                        arg.getMaxValue(),
                        k));
                k++;
                j *= temp.length;
            }
        }
        return overloadedArgs;
    }

    @Override
    public Table getTable(String name) {
        Table table = tables.get(name);
        if (table != null) {
            return table;
        }
        
        try {
            ColumnResolver x = FromCompiler.getResolver(
                    NamedTableNode.create(
                            null,
                            TableName.create(schemaName, name),
                            ImmutableList.<ColumnDef>of()), pc);
            final List<TableRef> tables = x.getTables();
            assert tables.size() == 1;
            TableRef tableRef = tables.get(0);
            if (!isView(tableRef.getTable())) {
                tableRef = fixTableMultiTenancy(tableRef);
                table = new PhoenixTable(pc, tableRef);
            }
        } catch (TableNotFoundException e) {
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        
        if (table == null) {
            table = resolveSequence(name);
        }
        
        if (table != null) {
            tables.put(name, table);
        }
        return table;
    }

    @Override
    public Set<String> getTableNames() {
        return tables.keySet();
    }

    @Override
    public Collection<Function> getFunctions(String name) {
        assert(!builtinFunctions.isEmpty());
        if(builtinFunctions.get(name) != null){
            return builtinFunctions.get(name);
        }
        Function func = views.get(name);
        if (func != null) {
            return ImmutableList.of(func);
        }
        try {
            List<String> functionNames = new ArrayList<String>(1);
            functionNames.add(name);
            ColumnResolver resolver = FromCompiler.getResolverForFunction(pc, functionNames);
            List<PFunction> pFunctions = resolver.getFunctions();
            assert !pFunctions.isEmpty();
            List<Function> funcs = new ArrayList<Function>(pFunctions.size());
            for (PFunction pFunction : pFunctions) {
                funcs.add(new PhoenixScalarFunction(pFunction));
            }
            return ImmutableList.copyOf(funcs);
        } catch (SQLException e) {
        }
        try {
            ColumnResolver x = FromCompiler.getResolver(
                    NamedTableNode.create(
                            null,
                            TableName.create(schemaName, name),
                            ImmutableList.<ColumnDef>of()), pc);
            final List<TableRef> tables = x.getTables();
            assert tables.size() == 1;
            final TableRef tableRef = tables.get(0);
            final PTable pTable = tableRef.getTable();
            if (isView(pTable)) {
                String viewSql = pTable.getViewStatement();
                if (viewSql == null) {
                    viewSql = "select * from "
                            + SchemaUtil.getEscapedFullTableName(
                                    pTable.getPhysicalName().getString());
                }
                SchemaPlus schema = parentSchema.getSubSchema(this.name);
                SchemaPlus viewSqlSchema =
                        this.schemaName == null ? schema : parentSchema;
                func = ViewTable.viewMacro(schema, viewSql,
                        CalciteSchema.from(viewSqlSchema).path(null),
                        null, pTable.getViewType() == ViewType.UPDATABLE);
                views.put(name, func);
                viewTables.add(tableRef);
            }
        } catch (TableNotFoundException e) {
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        
        return func == null ? Collections.<Function>emptyList() : ImmutableList.of(func);
    }

    @Override
    public Set<String> getFunctionNames() {
        return views.keySet();
    }

    @Override
    public Schema getSubSchema(String name) {
        if (schemaName != null) {
            return null;
        }
        
        Schema schema = subSchemas.get(name);
        if (schema != null) {
            return schema;
        }
        
        schema = new PhoenixSchema(name, name, parentSchema.getSubSchema(this.name), pc);
        subSchemas.put(name, schema);
        return schema;
    }

    @Override
    public Set<String> getSubSchemaNames() {
        return subSchemas.keySet();
    }

    @Override
    public Expression getExpression(SchemaPlus parentSchema, String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isMutable() {
        return true;
    }

    @Override
    public boolean contentsHaveChangedSince(long lastCheck, long now) {
        return lastCheck != now;
    }
    
    public void clear() {
        tables.clear();
        views.clear();
        this.views.put("ListJars", listJarsFunction);
        viewTables.clear();
    }
    
    public void defineIndexesAsMaterializations() {
        SchemaPlus schema = parentSchema.getSubSchema(this.name);
        SchemaPlus viewSqlSchema =
                this.schemaName == null ? schema : parentSchema;
        CalciteSchema calciteSchema = CalciteSchema.from(schema);
        List<String> path = CalciteSchema.from(viewSqlSchema).path(null);
        try {
            List<PhoenixTable> phoenixTables = Lists.newArrayList();
            for (Table table : tables.values()) {
                if (table instanceof PhoenixTable) {
                    phoenixTables.add((PhoenixTable) table);
                }
            }
            for (PhoenixTable phoenixTable : phoenixTables) {
                TableRef tableRef = phoenixTable.tableMapping.getTableRef();
                for (PTable index : tableRef.getTable().getIndexes()) {
                    TableRef indexTableRef = new TableRef(null, index,
                            tableRef.getTimeStamp(), tableRef.getLowerBoundTimeStamp(),
                            false);
                    addMaterialization(indexTableRef, path, calciteSchema);
                }
            }
            for (TableRef tableRef : viewTables) {
                final PTable pTable = tableRef.getTable();
                for (PTable index : pTable.getIndexes()) {
                    if (index.getParentName().equals(pTable.getName())) {
                        TableRef indexTableRef = new TableRef(null, index,
                                tableRef.getTimeStamp(), tableRef.getLowerBoundTimeStamp(),
                                false);
                        addMaterialization(indexTableRef, path, calciteSchema);
                    }
                }                
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    
    private void addMaterialization(TableRef indexTableRef, List<String> path,
            CalciteSchema calciteSchema) throws SQLException {
        indexTableRef = fixTableMultiTenancy(indexTableRef);
        final PhoenixTable table = new PhoenixTable(pc, indexTableRef);
        final PTable index = indexTableRef.getTable();
        tables.put(index.getTableName().getString(), table);
        StringBuffer sb = new StringBuffer();
        sb.append("SELECT");
        for (PColumn column : table.getColumns()) {
            String indexColumnName = column.getName().getString();
            String dataColumnName = IndexUtil.getDataColumnName(indexColumnName);
            sb.append(",").append(SchemaUtil.getEscapedFullColumnName(dataColumnName));
            sb.append(" ").append(SchemaUtil.getEscapedFullColumnName(indexColumnName));
        }
        sb.setCharAt(6, ' '); // replace first comma with space.
        sb.append(" FROM ").append(SchemaUtil.getEscapedFullTableName(index.getParentName().getString()));
        MaterializationService.instance().defineMaterialization(
                calciteSchema, null, sb.toString(), path, index.getTableName().getString(), true, true);        
    }
    
    private boolean isView(PTable table) {
        return table.getType() == PTableType.VIEW
                && table.getViewType() != ViewType.MAPPED;
    }
    
    private TableRef fixTableMultiTenancy(TableRef tableRef) throws SQLException {
        if (pc.getTenantId() != null || !tableRef.getTable().isMultiTenant()) {
            return tableRef;
        }
        PTable table = tableRef.getTable();
        table = PTableImpl.makePTable(
                table.getTenantId(), table.getSchemaName(), table.getTableName(), table.getType(), table.getIndexState(), table.getTimeStamp(),
                table.getSequenceNumber(), table.getPKName(), table.getBucketNum(), PTableImpl.getColumnsToClone(table), table.getParentSchemaName(), table.getParentTableName(),
                table.getIndexes(), table.isImmutableRows(), table.getPhysicalNames(), table.getDefaultFamilyName(), table.getViewStatement(),
                table.isWALDisabled(), false, table.getStoreNulls(), table.getViewType(), table.getViewIndexId(), table.getIndexType(),
                table.rowKeyOrderOptimizable(), table.isTransactional(), table.getUpdateCacheFrequency(), table.getBaseColumnCount(), table.getIndexDisableTimestamp(),
                table.isNamespaceMapped(), table.getAutoPartitionSeqName(), table.isAppendOnlySchema());
        return new TableRef(null, table, tableRef.getTimeStamp(),
                tableRef.getLowerBoundTimeStamp(), tableRef.hasDynamicCols());
    }
    
    private PhoenixSequence resolveSequence(String name) {
        try {
            sequenceManager.newSequenceReference(pc.getTenantId(),
                    TableName.createNormalized(schemaName, name) ,
                    null, SequenceValueParseNode.Op.NEXT_VALUE);
            sequenceManager.validateSequences(Sequence.ValueOp.VALIDATE_SEQUENCE);
        } catch (SQLException e){
            return null;
        } finally {
            sequenceManager.reset();
        }

        return new PhoenixSequence(schemaName, name, pc);
    }

    /** Schema factory that creates a
     * {@link org.apache.phoenix.calcite.PhoenixSchema}.
     * This allows you to create a Phoenix schema inside a model.json file.
     *
     * <pre>{@code
     * {
     *   version: '1.0',
     *   defaultSchema: 'HR',
     *   schemas: [
     *     {
     *       name: 'HR',
     *       type: 'custom',
     *       factory: 'org.apache.phoenix.calcite.PhoenixSchema.Factory',
     *       operand: {
     *         url: "jdbc:phoenix:localhost",
     *         user: "scott",
     *         password: "tiger"
     *       }
     *     }
     *   ]
     * }
     * }</pre>
     */
    public static class Factory implements SchemaFactory {
        public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
            return PhoenixSchema.create(parentSchema, name, operand);
        }
    }
}
