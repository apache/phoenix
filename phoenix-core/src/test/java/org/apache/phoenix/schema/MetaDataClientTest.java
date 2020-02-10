package org.apache.phoenix.schema;

import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.CreateTableStatement;
import org.apache.phoenix.parse.PSchema;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.junit.Test;
import java.sql.DriverManager;
import java.sql.SQLException;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;

public class MetaDataClientTest extends BaseConnectionlessQueryTest {

    @Test
    public void TestHandleCreateTableMutationCode() throws SQLException {
        String schema = generateUniqueName();
        String baseTable = generateUniqueName();
        PhoenixConnection phxConn = (PhoenixConnection) DriverManager.getConnection(getUrl());
        MetaDataClient mockClient = new MetaDataClient(phxConn);
        MetaDataProtocol.MetaDataMutationResult result = new MetaDataProtocol.MetaDataMutationResult(
            MetaDataProtocol.MutationCode.TABLE_NOT_IN_REGION ,new PSchema(schema),
            EnvironmentEdgeManager.currentTimeMillis());
        //Testing the case when Mutation code thrown from sever is not handled by MetaDataClient
        MetaDataProtocol.MetaDataMutationResult result1 = new MetaDataProtocol.MetaDataMutationResult(
                MetaDataProtocol.MutationCode.NO_PK_COLUMNS ,new PSchema(schema),
                EnvironmentEdgeManager.currentTimeMillis());
        String ddlFormat = "CREATE TABLE " + schema + "." + baseTable + " " +
            "(A VARCHAR PRIMARY KEY, B BIGINT, C VARCHAR)";
        CreateTableStatement stmt = (CreateTableStatement)new SQLParser((ddlFormat)).parseStatement();

        try {
            mockClient.handleCreateTableMutationCode(result, result.getMutationCode(), stmt, schema, baseTable, null);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TABLE_NOT_IN_REGION.getErrorCode(),e.getErrorCode());
        }
        try {
            mockClient.handleCreateTableMutationCode(result1, result1.getMutationCode(), stmt, schema, baseTable, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("NO_PK_COLUMNS"));
        }

    }

}
