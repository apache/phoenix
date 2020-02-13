package org.apache.phoenix.schema;

import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.CreateTableStatement;
import org.apache.phoenix.parse.PSchema;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.junit.BeforeClass;
import org.junit.Test;
import java.sql.DriverManager;
import java.sql.SQLException;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;

public class MetaDataClientTest extends BaseConnectionlessQueryTest {

    private static String schema;
    private static String baseTable;
    private static PhoenixConnection phxConn;
    private static MetaDataClient mockClient;
    private static String ddlFormat;
    private static CreateTableStatement stmt;

    @BeforeClass
    public static void setupTest() throws SQLException {
        schema = generateUniqueName();
        baseTable = generateUniqueName();
        phxConn = (PhoenixConnection) DriverManager.getConnection(getUrl());
        mockClient = new MetaDataClient(phxConn);
        ddlFormat = "CREATE TABLE " + schema + "." + baseTable + " " +
                "(A VARCHAR PRIMARY KEY, B BIGINT, C VARCHAR)";
        stmt = (CreateTableStatement)new SQLParser((ddlFormat)).parseStatement();
    }

    @Test
    public void testHandleCreateTableMutationCode() throws SQLException {
        MetaDataProtocol.MetaDataMutationResult result = new MetaDataProtocol.MetaDataMutationResult
                (MetaDataProtocol.MutationCode.UNALLOWED_TABLE_MUTATION ,new PSchema(schema),
            EnvironmentEdgeManager.currentTimeMillis());
        try {
            mockClient.handleCreateTableMutationCode(result, result.getMutationCode(), stmt,
                    schema, baseTable, null);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_MUTATE_TABLE.getErrorCode(),e.getErrorCode());
        }
    }

    @Test
    //Testing the case when Mutation code thrown from sever is not handled by MetaDataClient
    public void testHandleCreateTableMutationCodeWithNewCode() throws SQLException {
        MetaDataProtocol.MetaDataMutationResult result = new MetaDataProtocol.MetaDataMutationResult(
                MetaDataProtocol.MutationCode.NO_PK_COLUMNS ,new PSchema(schema),
                EnvironmentEdgeManager.currentTimeMillis());
        try {
            mockClient.handleCreateTableMutationCode(result, result.getMutationCode(), stmt,
                    schema, baseTable, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("NO_PK_COLUMNS"));
        }
    }

}
