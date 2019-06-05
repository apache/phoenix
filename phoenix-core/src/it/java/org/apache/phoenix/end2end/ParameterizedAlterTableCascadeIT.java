/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.end2end;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

@RunWith(Parameterized.class)
public class ParameterizedAlterTableCascadeIT extends ParallelStatsDisabledIT {

    @Rule
    public ExpectedException exception = ExpectedException.none();
    private static Connection conn;
    private Properties prop;
    boolean isViewIndex;
    String databaseObject;
    String databaseObjectName;
    String indexesName;
    String planOutput;

    public ParameterizedAlterTableCascadeIT(boolean isViewIndex) {
		this.isViewIndex = isViewIndex;
	}

	@Parameterized.Parameters(name = "ParameterizedAlterTableCascadeIT_isViewIndex={0}")
	public static Collection parametersList() {
		return Arrays.asList(new Object[] {
				true, false
		});
	}

    @Before
    public void setup() throws SQLException {
        prop = new Properties();
        conn = DriverManager.getConnection(getUrl(), prop);
        conn.setAutoCommit(true);
        conn.createStatement().execute("CREATE TABLE IF NOT EXISTS us_population (\n" +
                "      state CHAR(2) NOT NULL,\n" +
                "      city VARCHAR NOT NULL,\n" +
                "      population BIGINT,\n" +
                "      CONSTRAINT my_pk PRIMARY KEY (state, city)) COLUMN_ENCODED_BYTES=0");

        loadData();

		conn.createStatement().execute("CREATE VIEW IF NOT EXISTS us_population_gv" +
                "(city_area INTEGER, avg_fam_size INTEGER) AS " +
                "SELECT * FROM us_population WHERE state = 'CA'");

        if(isViewIndex) {

            conn.createStatement().execute("CREATE INDEX IF NOT EXISTS us_population_gv_gi ON " +
                    "us_population_gv (city_area) INCLUDE (population)");
            conn.createStatement().execute("CREATE INDEX IF NOT EXISTS us_population_gv_gi_2 ON " +
                    "us_population_gv (avg_fam_size) INCLUDE (population)");
            databaseObject = "VIEW";
            databaseObjectName = "us_population_gv";
            indexesName = "us_population_gv_gi, us_population_gv_gi_2";
            planOutput = "_IDX_US_POPULATION";

        } else {
            conn.createStatement().execute("CREATE INDEX IF NOT EXISTS us_population_gi ON " +
                    "us_population (population)");
            conn.createStatement().execute("CREATE INDEX IF NOT EXISTS us_population_gi_2 ON " +
                    "us_population (state, population)");
            databaseObject = "TABLE";
            databaseObjectName = "us_population";
            indexesName = "us_population_gi, us_population_gi_2";
            planOutput = "us_population_gi";
        }
    }

    private void loadData() throws SQLException {
        PreparedStatement ps = conn.prepareStatement("UPSERT INTO us_population VALUES('NY','New York',8143197)");
        ps.executeUpdate();
        ps = conn.prepareStatement("UPSERT INTO us_population VALUES('CA','Los Angeles',3844829)");
        ps.executeUpdate();
        ps = conn.prepareStatement("UPSERT INTO us_population VALUES('IL','Chicago',2842518)");
        ps.executeUpdate();
        ps = conn.prepareStatement("UPSERT INTO us_population VALUES('TX','Houston',2016582)");
        ps.executeUpdate();
        ps = conn.prepareStatement("UPSERT INTO us_population VALUES('PA','Philadelphia',1463281)");
        ps.executeUpdate();
        ps = conn.prepareStatement("UPSERT INTO us_population VALUES('AZ','Phoenix',1461575)");
        ps.executeUpdate();
        ps = conn.prepareStatement("UPSERT INTO us_population VALUES('TX','San Antonio',1256509)");
        ps.executeUpdate();
        ps = conn.prepareStatement("UPSERT INTO us_population VALUES('CA','San Diego',1255540)");
        ps.executeUpdate();
        ps = conn.prepareStatement("UPSERT INTO us_population VALUES('TX','Dallas',1213825)");
        ps.executeUpdate();
        ps = conn.prepareStatement("UPSERT INTO us_population VALUES('CA','San Jose',912332)");
        ps.executeUpdate();
    }

    // Test with ALTER TABLE CASCADE INDEX ALL
    @Test
    public void testAlterDBOAddCascadeIndexAll() throws SQLException {


        String query = "ALTER "+ databaseObject + " " + databaseObjectName +" ADD new_column VARCHAR CASCADE INDEX ALL";
        conn.createStatement().execute(query);
        query = "EXPLAIN SELECT new_column FROM "+databaseObjectName;
        ResultSet rs = conn.prepareStatement(query).executeQuery();
        rs.next();
        //confirm it is using global index to access the column
        Assert.assertTrue(rs.getString(1).contains(planOutput.toUpperCase()));
    }

    // Test with ALTER VIEW CASCADE INDEX ALL
    @Test
    public void testAlterDBOAddCascadeIndexAllUpsert() throws SQLException {
        String query = "ALTER "+ databaseObject + " " + databaseObjectName +" ADD new_column_3 VARCHAR CASCADE INDEX ALL";
        conn.createStatement().execute(query);
        PreparedStatement ps;
        if(isViewIndex) {
            ps = conn.prepareStatement("UPSERT INTO us_population_gv(state,city,population,city_area,avg_fam_size,new_column_3) " +
                    "VALUES('CA','Santa Barbara',912332,1300,4,'test_column')");
        } else {
            ps = conn.prepareStatement("UPSERT INTO us_population(state,city,population,new_column_3) " +
                    "VALUES('CA','Santa Barbara',912332,'test_column')");
        }
        ps.executeUpdate();
        query = "EXPLAIN SELECT new_column_3 FROM "+databaseObjectName+" where new_column_3 = 'test_column'";
        ResultSet rs = conn.prepareStatement(query).executeQuery();
        rs.next();
        //confirm it is using global index to access the column
        Assert.assertTrue(rs.getString(1).contains(planOutput.toUpperCase()));
        query = "SELECT new_column_3 FROM "+databaseObjectName+" where new_column_3 = 'test_column'";
        rs = conn.prepareStatement(query).executeQuery();
        rs.next();
        Assert.assertEquals(rs.getString(1),"test_column");
    }



    // Test with CASCADE INDEX <index_name>
    @Test
    public void testAlterDBOAddCascadeIndex() throws SQLException {
        String query = "ALTER "+databaseObject+" "+databaseObjectName + " ADD new_column_1 FLOAT CASCADE INDEX "+indexesName.split(",")[0];
        conn.createStatement().execute(query);
    }

    // Test with CASCADE INDEX <index_name>, <index_name>
    @Test
    public void testAlterDBOAddCascadeIndexes() throws SQLException {
        String query = "ALTER "+databaseObject+" "+databaseObjectName + " ADD new_column_1 DOUBLE CASCADE INDEX "+indexesName;
        conn.createStatement().execute(query);
    }

    // Exception for invalid grammar
    @Test
    public void testAlterDBOInvalidGrammarI() throws SQLException {
        String query = "ALTER "+databaseObject+" "+databaseObjectName +" ADD new_column VARCHAR ALL";
        exception.expectMessage("Syntax error");
        conn.createStatement().execute(query);
    }

    // Exception for invalid grammar
    @Test
    public void testAlterDBOInvalidGrammarII() throws SQLException {
        String query = "ALTER "+databaseObject+" "+databaseObjectName +" ADD new_column VARCHAR CASCADE "+indexesName.split(",")[0];
        exception.expectMessage("Syntax error");
        conn.createStatement().execute(query);
    }

    // Exception for invalid grammar
    @Test
    public void testAlterDBOInvalidGrammarIII() throws SQLException {
        String query = "ALTER "+databaseObject+" "+databaseObjectName +" ADD new_column VARCHAR INDEX "+indexesName.split(",")[0];
        exception.expectMessage("Syntax error");
        conn.createStatement().execute(query);
    }

    @After
    public void teardown() throws SQLException {
        if (isViewIndex) {
            conn.createStatement().execute("DROP INDEX us_population_gv_gi ON us_population_gv");
            conn.createStatement().execute("DROP INDEX us_population_gv_gi_2 ON us_population_gv");
        } else {
            conn.createStatement().execute("DROP INDEX us_population_gi ON us_population");
            conn.createStatement().execute("DROP INDEX us_population_gi_2 ON us_population");
        }
        conn.createStatement().execute("DROP VIEW us_population_gv");
        conn.createStatement().execute("DROP TABLE us_population");
    }
}
