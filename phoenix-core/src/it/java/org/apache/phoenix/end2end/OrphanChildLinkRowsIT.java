package org.apache.phoenix.end2end;

import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.ConnectionQueryServicesImpl;
import org.apache.phoenix.schema.TableAlreadyExistsException;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Map;

@Category(NeedsOwnMiniClusterTest.class)
public class OrphanChildLinkRowsIT extends BaseTest {

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    /**
     * 1. Create 2 tables - T1 and T2. Create a view V1 on T1. Create a view with the same name V1 on T2.
     * The second CREATE VIEW will fail, verify if there was no orphan child link because of that.
     *
     * 2. Instrument CQSI to fail phase three of CREATE VIEW. Crate a view V2 on T2 and V1 on T2 again which will fail.
     * Both links T2->V2 and T2->V1 will be in UNVERIFIED state, repaired during read.
     * Check if only 2 child links are returned: T2->V2 and T1->V1 from previous test.
     */
    @Test
    public void testNoOrphanChildLinkRow() throws Exception {
        String t1 = "CREATE TABLE S1.T1 (TENANT_ID VARCHAR NOT NULL, A INTEGER NOT NULL, B INTEGER CONSTRAINT PK PRIMARY KEY (TENANT_ID, A))";
        String t2 = "CREATE TABLE S2.T2 (TENANT_ID VARCHAR NOT NULL, A INTEGER NOT NULL, B INTEGER CONSTRAINT PK PRIMARY KEY (TENANT_ID, A))";
        String v1 = "CREATE VIEW VS1.V1 (NEW_COL1 INTEGER, NEW_COL2 INTEGER) AS SELECT * FROM S1.T1 WHERE B > 10";
        String v2 = "CREATE VIEW VS1.V1 (NEW_COL1 INTEGER, NEW_COL2 INTEGER) AS SELECT * FROM S2.T2 WHERE B > 10";

        try (Connection connection = DriverManager.getConnection(getUrl())) {
            connection.createStatement().execute(t1);
            connection.createStatement().execute(t2);
            connection.createStatement().execute(v1);
        }

        try (Connection connection = DriverManager.getConnection(getUrl())) {
            connection.createStatement().execute(v2);
        }
        catch (TableAlreadyExistsException e) {
        }

        verifyNoOrphanChildLinkRow(1);

        // configure CQSI to fail the last write phase of CREATE VIEW
        // where child link mutations are set to VERIFIED or are deleted
        ConnectionQueryServicesImpl.setFailPhaseThreeChildLinkWriteForTesting(true);
        String v3 = "CREATE VIEW VS2.V2 (NEW_COL1 INTEGER, NEW_COL2 INTEGER) AS SELECT * FROM S2.T2 WHERE B > 10";

        try (Connection connection = DriverManager.getConnection(getUrl())) {
            connection.createStatement().execute(v3);
            connection.createStatement().execute(v2);
        }
        catch (TableAlreadyExistsException e) {
        }
        verifyNoOrphanChildLinkRow(2);
    }


    private void verifyNoOrphanChildLinkRow(int expectedChildLinkRows) throws Exception {
        String childLinkQuery = "SELECT * FROM SYSTEM.CHILD_LINK";
        try (Connection connection = DriverManager.getConnection(getUrl())) {
            ResultSet rs = connection.createStatement().executeQuery(childLinkQuery);
            int count = 0;
            while (rs.next()) {
                //System.out.println(rs.getString(3) + " " + rs.getString(5));
                count++;
            }
            Assert.assertEquals("Found Orphan Linking Row", expectedChildLinkRows, count);
        }
    }


}
