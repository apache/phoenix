/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import static org.junit.Assert.assertTrue;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.UUID;

import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.util.SchemaUtil;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized.Parameters;

@Category(ParallelStatsDisabledTest.class)
public class UUIDTypeIT extends BaseQueryIT {

    protected static String SCHEMA1 = "SCHEMAFORUUIDTEST";

    public UUIDTypeIT(String indexDDL, boolean columnEncoded, boolean keepDeletedCells) {
        super(indexDDL, columnEncoded, keepDeletedCells);
    }
    
    @Parameters(name="UUIDTypeIT_{index}") // name is used by failsafe as file name in reports
    public static synchronized Collection<Object> data() {
        return BaseQueryIT.allIndexes();
    }       

    @Test
    public void testSimple() throws Exception {
        internalTestSimple(false, false); // No DESC in pk, neither in INDEX
        internalTestSimple(false, true); // No DESC in pk , DESC in INDEX
        internalTestSimple(true, false); // DESC in pk, No DESC in INDEX
        internalTestSimple(true, true); // DESC in pk, also in INDEX
    }

    public void internalTestSimple(boolean descInPk, boolean descInIndex) throws Exception {
        String baseTable = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());

        UUID uuidRecord1PK = UUID.randomUUID();
        UUID uuidRecord1UUID = UUID.randomUUID();
        
        UUID uuidRecord2PK = UUID.randomUUID();
        UUID uuidRecord2UUID = UUID.randomUUID();
        UUID uuidRecord2Array[]=new UUID[7];

        
        for(int x=0;x<uuidRecord2Array.length;x++)
            uuidRecord2Array[x]=UUID.randomUUID();
        

        
        UUID uuidRecord3PK = UUID.randomUUID();

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String baseTableDDL ="CREATE TABLE " + baseTable+ " (UUID_PK        UUID NOT NULL,  "
                                                                + "UUID_NOT_PK  UUID          , "
                                                                + "UUID_ARRAY   UUID ARRAY    , "
                                                                + "CONSTRAINT NAME_PK PRIMARY KEY(UUID_PK"+(descInPk?" DESC":"")+"))";
            conn.createStatement().execute(baseTableDDL);

            String upsert = "UPSERT INTO " + baseTable + "(UUID_PK , UUID_NOT_PK , UUID_ARRAY ) VALUES (?,?,?)";

            PreparedStatement ps = conn.prepareStatement(upsert);
            if (ps instanceof PhoenixPreparedStatement) {
                PhoenixPreparedStatement phops = (PhoenixPreparedStatement) ps;
                phops.setUUID(1, uuidRecord1PK);
                phops.setUUID(2, uuidRecord1UUID);
            } else {
                //This should be dead code
                ps.setObject(1, uuidRecord1PK);
                ps.setObject(2, uuidRecord1UUID);
            }
            ps.setArray(3, null);
            ps.execute();

            ps = conn.prepareStatement(upsert);
            //Let's do it without casting to PhoenixPreparedStatement
            ps.setObject(1, uuidRecord2PK);
            ps.setObject(2, uuidRecord2UUID);
            Array array = conn.createArrayOf("UUID", uuidRecord2Array);
            ps.setArray(3, array);
            ps.execute();
            
            
            ps = conn.prepareStatement(upsert);
            //Let's do it without casting to PhoenixPreparedStatement
            ps.setObject(1, uuidRecord3PK);
            ps.setObject(2, null);
            ps.setArray(3, null);
            ps.execute();


            conn.commit();

            
            ///////////////////////////////////////////////////////////////////////////
            // First let's do queries without an index, so UUID_NOT_PK is not still an UUID_INDEXABLE in an index
            ///////////////////////////////////////////////////////////////////////////
            
            
            ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + baseTable);
            rs.next();
            assertEquals(rs.getInt(1), 3);
            
            // Testing over PK
            
            ps = conn.prepareStatement("SELECT UUID_PK , UUID_NOT_PK FROM " + baseTable+" where UUID_PK = ?");
            ps.setObject(1, uuidRecord1PK);
            rs = ps.executeQuery();
            
            assertTrue(rs.next());
            assertTrue(uuidRecord1PK.equals(((PhoenixResultSet)rs).getUUID(1)));  // with PhoenixResultSet
            assertTrue(uuidRecord1UUID.equals(rs.getObject(2)));  // with standard ResultSet
            assertFalse(rs.next());
            
            ps = conn.prepareStatement("SELECT UUID_PK , UUID_NOT_PK FROM " + baseTable+" where UUID_PK = STR_TO_UUID(?)");
            ps.setObject(1, uuidRecord3PK.toString());
            rs = ps.executeQuery();
            
            assertTrue(rs.next());
            assertTrue(uuidRecord3PK.equals(((PhoenixResultSet)rs).getUUID(1)));  // with PhoenixResultSet
            assertTrue(rs.getObject(2)==null);  // with standard ResultSet
            assertFalse(rs.next());            

            ps = conn.prepareStatement("SELECT UUID_PK , UUID_NOT_PK FROM " + baseTable+" where  UUID_TO_STR(UUID_PK) = UUID_TO_STR(?) ");
            ((PhoenixPreparedStatement)ps).setUUID(1, uuidRecord3PK);
            rs = ps.executeQuery();
            
            assertTrue(rs.next());
            assertTrue(uuidRecord3PK.equals(((PhoenixResultSet)rs).getUUID(1)));  // with PhoenixResultSet
            assertTrue(rs.getObject(2)==null);  // with standard ResultSet
            assertFalse(rs.next()); 
            
            
            // Testing over NON pk
            
            rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + baseTable+" where UUID_NOT_PK IS NOT NULL");
            rs.next();
            assertEquals(rs.getInt(1), 2);
            

            rs = conn.createStatement().executeQuery("SELECT UUID_PK , UUID_NOT_PK FROM " + baseTable+" where UUID_NOT_PK IS NULL");
            assertTrue(rs.next());
            assertTrue(uuidRecord3PK.equals(rs.getObject(1)));  // with PhoenixResultSet
            assertTrue(rs.getObject(2)==null);  // with standard ResultSet
            assertFalse(rs.next());
            
            ps = conn.prepareStatement("SELECT UUID_PK , UUID_NOT_PK , UUID_ARRAY FROM " + baseTable+" where UUID_NOT_PK = ?");
            ps.setObject(1, uuidRecord2UUID);
            rs = ps.executeQuery();
            assertTrue(rs.next());
            assertTrue(uuidRecord2PK.equals(rs.getObject(1)));  // with PhoenixResultSet
            assertTrue(uuidRecord2UUID.equals(((PhoenixResultSet)rs).getUUID(2)));
            Array temp = rs.getArray(3);
            assertThat(uuidRecord2Array,is(temp.getArray()));
            
            assertFalse(rs.next());

            baseTableDDL =
                    "CREATE INDEX RANDOMINDEXNAME55454 ON " + baseTable + " (UUID_NOT_PK"
                            + (descInIndex ? " DESC" : "") + ")";
            
            conn.createStatement().execute(baseTableDDL);

            ///////////////////////////////////////////////////////////////////////////
            // Now, there is an index; so, next lookups for  UUID_NOT_PK will be used as UUID_INDEXABLE in the index RANDOMINDEXNAME55454
            ///////////////////////////////////////////////////////////////////////////
            
            
            // Testing over NON pk
            
            rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + baseTable+" where UUID_NOT_PK IS NOT NULL");
            rs.next();
            assertEquals(rs.getInt(1), 2);
            

            rs = conn.createStatement().executeQuery("SELECT UUID_PK , UUID_NOT_PK FROM " + baseTable+" where UUID_NOT_PK IS NULL");
            assertTrue(rs.next());
            assertTrue(uuidRecord3PK.equals(rs.getObject(1)));  // with PhoenixResultSet
            assertTrue(rs.getObject(2)==null);  // with standard ResultSet
            assertFalse(rs.next());
            
            ps = conn.prepareStatement("SELECT UUID_PK , UUID_NOT_PK FROM " + baseTable+" where UUID_NOT_PK = ?");
            ps.setObject(1, uuidRecord2UUID);
            rs = ps.executeQuery();
            assertTrue(rs.next());
            assertTrue(uuidRecord2PK.equals(rs.getObject(1)));  // with PhoenixResultSet
            assertTrue(uuidRecord2UUID.equals(((PhoenixResultSet)rs).getUUID(2))); 
            assertFalse(rs.next());

        } finally {

            try (Connection conn = DriverManager.getConnection(getUrl())) {
                String baseTableDDL = "DROP TABLE  IF EXISTS " + baseTable;
                conn.createStatement().execute(baseTableDDL);
            }

        }

    }



}
