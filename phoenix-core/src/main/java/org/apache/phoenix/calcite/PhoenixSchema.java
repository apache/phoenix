package org.apache.phoenix.calcite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.materialize.MaterializationService;
import org.apache.calcite.schema.*;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.parse.ColumnDef;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.ViewType;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.IndexUtil;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
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
    protected final MetaDataClient client;
    
    protected final Set<String> subSchemaNames;
    protected final Map<String, PTable> tableMap;
    protected final Map<String, ViewDef> viewDefMap;
    protected final Map<String, Function> functionMap;
    
    private PhoenixSchema(String name, String schemaName, PhoenixConnection pc) {
        this.name = name;
        this.schemaName = schemaName;
        this.pc = pc;
        this.client = new MetaDataClient(pc);
        this.tableMap = Maps.<String, PTable> newHashMap();
        this.viewDefMap = Maps.<String, ViewDef> newHashMap();
        this.functionMap = Maps.<String, Function> newHashMap();
        loadTables();
        this.subSchemaNames = schemaName == null ? 
                  ImmutableSet.<String> copyOf(loadSubSchemaNames()) 
                : Collections.<String> emptySet();
    }
    
    private Set<String> loadSubSchemaNames() {
        try {
            DatabaseMetaData md = pc.getMetaData();
            ResultSet rs = md.getSchemas();
            Set<String> subSchemaNames = Sets.newHashSet();
            while (rs.next()) {
                String schemaName = rs.getString(PhoenixDatabaseMetaData.TABLE_SCHEM);
                subSchemaNames.add(schemaName == null ? "" : schemaName);
            }
            return subSchemaNames;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    
    private void loadTables() {
        try {
            DatabaseMetaData md = pc.getMetaData();
            ResultSet rs = md.getTables(null, schemaName == null ? "" : schemaName, null, null);
            while (rs.next()) {
                String tableName = rs.getString(PhoenixDatabaseMetaData.TABLE_NAME);
                String tableType = rs.getString(PhoenixDatabaseMetaData.TABLE_TYPE);
                if (!tableType.equals(PTableType.VIEW.getValue().getString())) {
                    ColumnResolver x = FromCompiler.getResolver(
                            NamedTableNode.create(
                                    null,
                                    TableName.create(schemaName, tableName),
                                    ImmutableList.<ColumnDef>of()), pc);
                    final List<TableRef> tables = x.getTables();
                    assert tables.size() == 1;
                    tableMap.put(tableName, tables.get(0).getTable());
                } else {
                    String viewSql = rs.getString(PhoenixDatabaseMetaData.VIEW_STATEMENT);
                    String viewType = rs.getString(PhoenixDatabaseMetaData.VIEW_TYPE);
                    viewDefMap.put(tableName, new ViewDef(viewSql, viewType.equals(ViewType.UPDATABLE.name())));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static Schema create(String name, Map<String, Object> operand) {
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
            return new PhoenixSchema(name, null, phoenixConnection);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Table getTable(String name) {
        PTable table = tableMap.get(name);
        return table == null ? null : new PhoenixTable(pc, table);
    }

    @Override
    public Set<String> getTableNames() {
        return tableMap.keySet();
    }

    @Override
    public Collection<Function> getFunctions(String name) {
        Function func = functionMap.get(name);
        return func == null ? Collections.<Function>emptyList() : ImmutableList.of(func);
    }

    @Override
    public Set<String> getFunctionNames() {
        return viewDefMap.keySet();
    }

    @Override
    public Schema getSubSchema(String name) {
        if (!subSchemaNames.contains(name))
            return null;
        
        return new PhoenixSchema(name, name, pc);
    }

    @Override
    public Set<String> getSubSchemaNames() {
        return subSchemaNames;
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
        return false;
    }
    
    public void initFunctionMap(CalciteSchema calciteSchema) {
        for (Map.Entry<String, ViewDef> entry : viewDefMap.entrySet()) {
            ViewDef viewDef = entry.getValue();
            Function func = ViewTable.viewMacro(
                    calciteSchema.plus(), viewDef.viewSql,
                    calciteSchema.path(null), viewDef.updatable);
            functionMap.put(entry.getKey(), func);
        }
    }
    
    public void defineIndexesAsMaterializations(CalciteSchema calciteSchema) {
        List<String> path = calciteSchema.path(null);
        for (PTable table : tableMap.values()) {
            for (PTable index : table.getIndexes()) {
                addMaterialization(table, index, path, calciteSchema);
            }
        }
    }
    
    protected void addMaterialization(PTable table, PTable index, List<String> path,
            CalciteSchema calciteSchema) {
        StringBuffer sb = new StringBuffer();
        sb.append("SELECT");
        for (int i = PhoenixTable.getStartingColumnPosition(index); i < index.getColumns().size(); i++) {
            PColumn column = index.getColumns().get(i);
            String indexColumnName = column.getName().getString();
            String dataColumnName = IndexUtil.getDataColumnName(indexColumnName);
            sb.append(",").append("\"").append(dataColumnName).append("\"");
            sb.append(" ").append("\"").append(indexColumnName).append("\"");
        }
        sb.setCharAt(6, ' '); // replace first comma with space.
        sb.append(" FROM ").append("\"").append(table.getTableName().getString()).append("\"");
        MaterializationService.instance().defineMaterialization(
                calciteSchema, null, sb.toString(), path, index.getTableName().getString(), true, true);        
    }
    
    private static class ViewDef {
        final String viewSql;
        final boolean updatable;
        
        ViewDef(String viewSql, boolean updatable) {
            this.viewSql = viewSql;
            this.updatable = updatable;
        }
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
            return PhoenixSchema.create(name, operand);
        }
    }
}
