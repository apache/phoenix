package org.apache.phoenix.calcite;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.*;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.ColumnDef;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.TableRef;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

/**
 * Implementation of Calcite's {@link Schema} SPI for Phoenix.
 */
public class PhoenixSchema implements Schema {
    public static final Factory FACTORY = new Factory();
    
    private final String schemaName;
    private final PhoenixConnection pc;
    protected final MetaDataClient client;
    
    // TODO to be removed after PHOENIX-1878.
    private static final SetMultimap<String, String> tableCache;
    static {
        tableCache = HashMultimap.<String, String> create();
        tableCache.put("", "ATABLE");
        tableCache.put("Join", "ItemTable");
        tableCache.put("Join", "SupplierTable");
        tableCache.put("Join", "CustomerTable");
        tableCache.put("Join", "OrderTable");
    }
    
    private PhoenixSchema(String name, PhoenixConnection pc) {
        this.schemaName = name;
        this.pc = pc;
        this.client = new MetaDataClient(pc);
    }

    private static Schema create(SchemaPlus parentSchema, Map<String, Object> operand) {
        String url = (String) operand.get("url");
        final Properties properties = new Properties();
        for (Map.Entry<String, Object> entry : operand.entrySet()) {
            properties.setProperty(entry.getKey(), String.valueOf(entry.getValue()));
        }
        try {
            final Connection connection =
                DriverManager.getConnection(url, properties);
            final PhoenixConnection phoenixConnection =
                connection.unwrap(PhoenixConnection.class);
            return new PhoenixSchema(null, phoenixConnection);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Table getTable(String name) {
        try {
            ColumnResolver x = FromCompiler.getResolver(
                NamedTableNode.create(
                    null,
                    TableName.create(schemaName, name),
                    ImmutableList.<ColumnDef>of()), pc);
            final List<TableRef> tables = x.getTables();
            assert tables.size() == 1;
            return new PhoenixTable(pc, tables.get(0).getTable());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<String> getTableNames() {
        return tableCache.get(schemaName == null ? "" : schemaName);
    }

    @Override
    public Collection<Function> getFunctions(String name) {
        return ImmutableSet.of();
    }

    @Override
    public Set<String> getFunctionNames() {
        return ImmutableSet.of();
    }

    @Override
    public Schema getSubSchema(String name) {
        if (schemaName != null || !tableCache.containsKey(name))
            return null;
        
        return new PhoenixSchema(name, pc);
    }

    @Override
    public Set<String> getSubSchemaNames() {
        if (schemaName != null)
            return Collections.emptySet();
        
        return tableCache.keySet();
    }

    @Override
    public Expression getExpression(SchemaPlus parentSchema, String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isMutable() {
        return false;
    }

    @Override
    public boolean contentsHaveChangedSince(long lastCheck, long now) {
        return false;
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
            return PhoenixSchema.create(parentSchema, operand);
        }
    }
}
