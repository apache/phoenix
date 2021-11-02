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

import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(ParallelStatsDisabledTest.class)
public class BinaryTypeIT  extends ParallelStatsDisabledIT  {

    @Test
    public void testBinaryNullAssignment() throws SQLException {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        
        ResultSet rs;
        
        try (Statement stmt = conn.createStatement()) {
        
            String binTestTable=generateUniqueName();
            
            stmt.execute("create table "+binTestTable+" (id integer not null, text varchar(255), testbin binary(16), CONSTRAINT pk primary key (id))");
            conn.commit();
        
            String queryIsNull = "select id, text , testbin from "+binTestTable+" where testbin is null";
        
            
            // Let's see if without using it, it is stored as null
            stmt.execute("upsert into "+binTestTable+"  (id,text) values (1,'anytext')");
            conn.commit();
            rs= stmt.executeQuery(queryIsNull);
            assertTrue(rs.next());
            rs.close();
            
            // Let's see if using it, but it is set as null,  it is also stored as null
            stmt.execute("upsert into "+binTestTable+"  (id,text,testbin) values (1,'anytext',null)");
            conn.commit();
            rs = stmt.executeQuery(queryIsNull);
            assertTrue(rs.next());
            rs.close();
            
            //Now let's set a value. Now It should be NOT null
            stmt.execute("upsert into "+binTestTable+"  (id,text,testbin) values (1,'anytext','a')");
            conn.commit();
            rs = stmt.executeQuery(queryIsNull);
            assertTrue(false == rs.next());
            rs.close();
            
            //Right now it has a value.... let's see if we can set it again a null value
            stmt.execute("upsert into "+binTestTable+"  (id,text,testbin) values (1,'anytext',null)");
            conn.commit();
            rs = stmt.executeQuery(queryIsNull);
            assertTrue(rs.next());
            rs.close();
           
        
            stmt.execute("DROP TABLE "+binTestTable+" ");
            conn.commit();
            
            rs.close();
        }
    }
}
