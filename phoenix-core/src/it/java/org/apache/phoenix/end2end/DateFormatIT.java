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

import org.apache.phoenix.query.QueryServices;
import org.junit.Test;

import java.sql.*;
import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Properties;

public class DateFormatIT extends ParallelStatsDisabledIT {
    @Test
    public void testDate() throws SQLException, InterruptedException {
        Thread t1 = new MyThread("GMT", "table1", getUrl());
        Thread t2 = new MyThread("GMT+8","table2", getUrl());
        t1.start();
        t2.start();
        t2.join();
        t1.join();
    }
}


class MyThread extends Thread {

    private String timeZone;
    private String url;
    private String tableName;


    public MyThread(String timeZone, String tablename, String url) {
        this.timeZone = timeZone;
        this.url = url;
        this.tableName = tablename;
    }

    @Override
    public void run() {
        try {
            String ddl = "CREATE TABLE IF NOT EXISTS " + tableName + " (k1 INTEGER PRIMARY KEY," +
                    " v_date DATE, v_time TIME, v_timestamp TIMESTAMP)";
            Properties properties = new Properties();
            properties.put(QueryServices.DATE_FORMAT_TIMEZONE_ATTRIB, timeZone);
            Connection conn = null;
            try {
                conn = DriverManager.getConnection(url, properties);
            } catch (SQLException e) {
                e.printStackTrace();
            }

            conn.createStatement().execute("drop table if exists " + tableName);
            conn.createStatement().execute(ddl);

            // We don't want to display the Milliseconds, because of the Date format
            long currentMs = System.currentTimeMillis() - System.currentTimeMillis() % 1000;
            java.sql.Date date = new Date(currentMs);

            DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String dateStr = df.format(date);
            java.sql.Time time = new Time(currentMs);
            java.sql.Timestamp ts = new Timestamp(currentMs);

            String dml = "UPSERT INTO " + tableName + " VALUES (" +
                    "1," +
                    "'" + dateStr + "'," +
                    "'" + dateStr + "'," +
                    "'" + dateStr + "'" +
                    ")";
            System.out.println(dml);
            conn.createStatement().executeUpdate(dml);
            conn.commit();

            String dml2 = "UPSERT INTO " + tableName + " VALUES (" +
                    "2," +
                    "to_date('" + dateStr + "')," +
                    "to_time('" + dateStr + "')," +
                    "to_timestamp('" + dateStr + "')" +
                    ")";
            System.out.println(dml2);
            conn.createStatement().executeUpdate(dml2);
            conn.commit();

            PreparedStatement stmt = conn.prepareStatement("upsert into " + tableName +
                    " values(?,?,?,?)");
            stmt.setInt(1, 3);
            stmt.setDate(2, date);
            stmt.setTime(3, time);
            stmt.setTimestamp(4, ts);
            stmt.executeUpdate();
            conn.commit();

            System.out.println("Print table data:");
            Statement stmt2 = conn.createStatement();
            ResultSet rs = stmt2.executeQuery("select * from " + tableName);
            Result result = null;
            String resTime = null;
            while (rs.next()) {
//            if (result == null){
//                result = new Result(rs.getDate(2), rs.getTime(3), rs.getTimestamp(4));
//                resTime = result.getTimestamp().toString();
//            }
//            else {
//                assertEquals(rs.getDate(2), result.getDate());
//                assertEquals(rs.getTime(3), result.getTime());
//                assertEquals(rs.getTimestamp(4), result.getTimestamp());
//            }
                System.out.println(timeZone + ": " + rs.getInt(1) + " | " + rs.getDate(2)
                        + " | " + rs.getTime(3) + " | " + rs.getTimestamp(4));
            }

            rs = stmt.executeQuery("select * from " + tableName + " WHERE v_timestamp = time'" + ts + "'");
            result = null;
            while (rs.next()) {
//            if (result == null){
//                result = new Result(rs.getString(2), rs.getString(3), rs.getString(4));
//                assertEquals(time, result.getTimeString().substring(0, time.length()));
//            }
//            else {
//                assertEquals(rs.getString(2), result.getDateString());
//                assertEquals(rs.getString(3), result.getTimeString());
//                assertEquals(rs.getString(4), result.getTimestampString());
//            }
                System.out.println(timeZone + ": " + rs.getString(1) + " | " + rs.getString(2)
                        + " | " + rs.getString(3) + " | " + rs.getString(4));
            }
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

class Result{
    private Date date;
    private Time time;
    private Timestamp timestamp;
    private String dateString;
    private String timeString;
    private String timestampString;

    Result(Date date, Time time, Timestamp timestamp) {
        this.date = date;
        this.time = time;
        this.timestamp = timestamp;
    }

    Result(String date, String time, String timestamp) {
        this.dateString = date;
        this.timeString = time;
        this.timestampString = timestamp;
    }

    public Date getDate() {
        return date;
    }

    public Time getTime() {
        return time;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public String getDateString() {
        return dateString;
    }

    public String getTimeString() {
        return timeString;
    }

    public String getTimestampString() {
        return timestampString;
    }
}
