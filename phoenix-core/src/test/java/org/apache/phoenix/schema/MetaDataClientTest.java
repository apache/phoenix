package org.apache.phoenix.schema;

import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.CreateTableStatement;
import org.apache.phoenix.parse.PSchema;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import static junit.framework.Assert.fail;
import static junit.framework.TestCase.assertEquals;
import static org.mockito.Matchers.any;

public class MetaDataClientTest extends BaseConnectionlessQueryTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetaDataClientTest.class);

    @Test
    public void testCreateTable() throws SQLException {
        String schema = generateUniqueName();
        String baseTable = generateUniqueName();
        PhoenixConnection phxConn = (PhoenixConnection) DriverManager.getConnection(getUrl());
        MetaDataClient client = new MetaDataClient(phxConn);
        MetaDataClient spyClient = Mockito.spy(client);
        MetaDataProtocol.MetaDataMutationResult result = new MetaDataProtocol.MetaDataMutationResult(
            MetaDataProtocol.MutationCode.TABLE_NOT_IN_REGION ,new PSchema(schema),
            EnvironmentEdgeManager.currentTimeMillis());
        String ddlFormat = "CREATE TABLE " + schema + "." + baseTable + " " +
            "(A VARCHAR PRIMARY KEY, B BIGINT, C VARCHAR)";
        CreateTableStatement stmt = (CreateTableStatement)new SQLParser((ddlFormat)).parseStatement();

        try {
            Mockito.when(spyClient.getCreateTableMutationResult(Mockito.anyList(),
                any(PTable.ViewType.class),Mockito.anyBoolean(), Mockito.anyList(),
                any(PTableType.class), Mockito.anyMap(), Mockito.anyList(), any(byte[][].class),
                Mockito.anyBoolean(), any(PTable.class))).thenReturn(result);
            spyClient.createTableInternal(stmt, null, null, null,null, PDataType.fromLiteral("BIGINT"),
               null, null, false, null, null, new HashMap<>(), new HashMap<>());
            fail();
        } catch (SQLException e) {
            LOGGER.debug("Exception:" + e.getMessage());
            assertEquals(e.getErrorCode(), SQLExceptionCode.TABLE_NOT_IN_REGION.getErrorCode());
        }
    }

}
