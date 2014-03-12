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
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import org.junit.Test;

import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.TableAlreadyExistsException;
import org.apache.phoenix.util.PhoenixRuntime;

public class CreateTableIT extends BaseClientManagedTimeIT {
    
    @Test
    public void testStartKeyStopKey() throws SQLException {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE TABLE start_stop_test (pk char(2) not null primary key) SPLIT ON ('EA','EZ')");
        conn.close();
        
        String query = "select count(*) from start_stop_test where pk >= 'EA' and pk < 'EZ'";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        conn = DriverManager.getConnection(getUrl(), props);
        Statement statement = conn.createStatement();
        statement.execute(query);
        PhoenixStatement pstatement = statement.unwrap(PhoenixStatement.class);
        List<KeyRange>splits = pstatement.getQueryPlan().getSplits();
        assertTrue(splits.size() > 0);
    }
    
    @Test
    public void testCreateTable() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = "CREATE TABLE m_interface_job(                data.addtime VARCHAR ,\n" + 
                "                data.dir VARCHAR ,\n" + 
                "                data.end_time VARCHAR ,\n" + 
                "                data.file VARCHAR ,\n" + 
                "                data.fk_log VARCHAR ,\n" + 
                "                data.host VARCHAR ,\n" + 
                "                data.row VARCHAR ,\n" + 
                "                data.size VARCHAR ,\n" + 
                "                data.start_time VARCHAR ,\n" + 
                "                data.stat_date DATE ,\n" + 
                "                data.stat_hour VARCHAR ,\n" + 
                "                data.stat_minute VARCHAR ,\n" + 
                "                data.state VARCHAR ,\n" + 
                "                data.title VARCHAR ,\n" + 
                "                data.user VARCHAR ,\n" + 
                "                data.inrow VARCHAR ,\n" + 
                "                data.jobid VARCHAR ,\n" + 
                "                data.jobtype VARCHAR ,\n" + 
                "                data.level VARCHAR ,\n" + 
                "                data.msg VARCHAR ,\n" + 
                "                data.outrow VARCHAR ,\n" + 
                "                data.pass_time VARCHAR ,\n" + 
                "                data.type VARCHAR ,\n" + 
                "                id INTEGER not null primary key desc\n" + 
                "                ) ";
        conn.createStatement().execute(ddl);
        conn = DriverManager.getConnection(getUrl(), props);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        try {
            conn.createStatement().execute(ddl);
            fail();
        } catch (TableAlreadyExistsException e) {
            // expected
        }
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 20));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("DROP TABLE m_interface_job");
    }
}
