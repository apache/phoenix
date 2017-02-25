package org.apache.phoenix.calcite;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.phoenix.calcite.rel.PhoenixRelImplementor;
import org.apache.phoenix.calcite.rel.PhoenixRelImplementorImpl;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.ExpressionCompiler;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.execute.RuntimeContext;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.TableRef;
import org.junit.Test;


public class ToExpressionTest extends BaseConnectionlessQueryTest {
	
	@Test
	public void toExpressionTest() throws Exception {
		new ExpressionChecker(
		        "T", 
		        "CREATE TABLE t(k1 VARCHAR PRIMARY KEY, k2 VARCHAR, v1 VARCHAR)", 
		        "SELECT * FROM T WHERE K2 = 'foo'", 
		        new WhereExpressionGetter())
		    .checkExpressionEquality()
		    .checkTypeEquality();
	}

	private static class ExpressionChecker {
	    private final PTable table;
	    private final Expression phoenixExpr;
	    private final RexNode calciteExpr;
	    private final StatementContext context;

	    public ExpressionChecker(String tableName, String ddl, String sql, ExpressionGetter getter) throws Exception {
	        Connection conn = DriverManager.getConnection(getOldUrl());
	        PhoenixConnection pc = conn.unwrap(PhoenixConnection.class);
	        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
	        this.context = new StatementContext(stmt);

	        conn.createStatement().execute(ddl);
	        this.table = pc.getMetaDataCache().getTableRef(new PTableKey(null,tableName)).getTable();
	        SelectStatement select = new SQLParser(sql).parseQuery();
	        ColumnResolver resolver = FromCompiler.getResolverForQuery(select, pc);
	        this.phoenixExpr = getter.getParseNode(select).accept(new ExpressionCompiler(new StatementContext(stmt, resolver)));

	        Planner planner = getPlanner(pc, table);
	        SqlNode sqlNode = planner.parse(sql);
	        sqlNode = planner.validate(sqlNode);
	        RelNode rel = planner.convert(sqlNode);
	        this.calciteExpr = getter.getRexNode(rel);
	    }

	    public ExpressionChecker checkExpressionEquality() {        
	        PhoenixRelImplementor implementor =
	                new PhoenixRelImplementorImpl(context, RuntimeContext.EMPTY_CONTEXT);
	        implementor.setTableMapping(new TableMapping(table));
	        Expression e = CalciteUtils.toExpression(this.calciteExpr, implementor);
	        assertEquals(this.phoenixExpr,e);
	        return this;
	    }
	    
	    public ExpressionChecker checkTypeEquality() {
	        // TODO
	        return this;
	    }
	}
	
	private static interface ExpressionGetter {
	    ParseNode getParseNode(SelectStatement select);
	    RexNode getRexNode(RelNode rel);
	}
	
	private static class WhereExpressionGetter implements ExpressionGetter {
        @Override
        public ParseNode getParseNode(SelectStatement select) {
            return select.getWhere();
        }
        @Override
        public RexNode getRexNode(RelNode rel) {
            Filter filter = null;
            while (filter == null && rel != null) {
                if (rel instanceof Filter) {
                    filter = (Filter) rel;
                } else if (!rel.getInputs().isEmpty()) {
                    rel = rel.getInput(0);
                } else {
                    rel = null;
                }
            }
            if (filter == null) {
                throw new RuntimeException("Couldn't find Filter rel.");
            }
            
            return filter.getCondition();
        }
	}

	private static Planner getPlanner(PhoenixConnection pc, PTable... tables) {
	    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
	    final FrameworkConfig config = Frameworks.newConfigBuilder()
	            .parserConfig(SqlParser.Config.DEFAULT)
	            .defaultSchema(rootSchema.add("phoenix",
	                    TestSchema.create(pc, tables)))
	            .build();
	    return Frameworks.getPlanner(config);
	}
	
	private static class TestSchema implements Schema {
	    private final PhoenixConnection pc;
	    private final Map<String, Map<String, PTable>> tableMap;
	    
	    public static TestSchema create(final PhoenixConnection pc, final PTable[] tables) {
	        Map<String, Map<String, PTable>> tableMap = new HashMap<String, Map<String, PTable>>();
	        for (PTable table : tables) {
	            PName schemaName = table.getSchemaName();
	            PName tableName = table.getTableName();
	            String schemaStr = schemaName == null ? "" : schemaName.getString();
	            Map<String, PTable> subSchemaMap = tableMap.get(schemaStr);
	            if (subSchemaMap == null) {
	                subSchemaMap = new HashMap<String, PTable>();
	                tableMap.put(schemaStr, subSchemaMap);
	            }
	            subSchemaMap.put(tableName.getString(), table);
	        }
	        return new TestSchema(pc, tableMap);
	    }
	    
	    private TestSchema(final PhoenixConnection pc, final Map<String, Map<String, PTable>> tableMap) {
	        this.pc = pc;
	        this.tableMap = tableMap;
	    }

        @Override
        public Table getTable(String name) {
            Map<String, PTable> rootTables = tableMap.get("");
            if (rootTables == null)
                return null;
            
            PTable table = rootTables.get(name);
            try {
                return new PhoenixTable(pc, new TableRef(table), null);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Set<String> getTableNames() {
            Map<String, PTable> rootTables = tableMap.get("");
            return rootTables == null ? Collections.<String>emptySet() : rootTables.keySet();
        }

        @Override
        public Collection<Function> getFunctions(String name) {
            return Collections.emptySet();
        }

        @Override
        public Set<String> getFunctionNames() {
            return Collections.emptySet();
        }

        @Override
        public Schema getSubSchema(String name) {
            Map<String, PTable> subMap = tableMap.get(name);
            if (subMap == null)
                return null;
            
            Map<String, Map<String, PTable>> tableMap = new HashMap<String, Map<String, PTable>>();
            tableMap.put("", subMap);
            return new TestSchema(pc, tableMap);
        }

        @Override
        public Set<String> getSubSchemaNames() {
            return tableMap.keySet();
        }

        @Override
        public org.apache.calcite.linq4j.tree.Expression getExpression(
                SchemaPlus parentSchema, String name) {
            return null;
        }

        @Override
        public boolean isMutable() {
            return false;
        }

        @Override
        public boolean contentsHaveChangedSince(long lastCheck, long now) {
            return false;
        }
	    
	}
}
