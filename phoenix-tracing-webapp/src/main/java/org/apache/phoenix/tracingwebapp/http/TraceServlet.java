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
package org.apache.phoenix.tracingwebapp.http;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;

/**
 *
 * Server to show trace information
 *
 *
 * @since 4.5.5
 */

public class TraceServlet extends HttpServlet {
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    response.setContentType("text/html");
    connectMe();
    response.setStatus(HttpServletResponse.SC_OK);
    response.getWriter().println("<h1>Trace Servlet</h1>");
    response.getWriter().println("session=" + request.getSession(true).getId());

  }

  public void connectMe() {
    Statement stmt = null;
    ResultSet rset = null;
    try {
      Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
      Connection con = DriverManager
          .getConnection("jdbc:phoenix:localhost:2181");
      stmt = con.createStatement();
      //TO-DO remove sample tabel test when reading trace
      stmt.executeUpdate("CREATE TABLE IF NOT EXISTS test (mykey integer not null primary key, mycolumn varchar)");
      stmt.executeUpdate("upsert into test values (1,'Sample Data')");
      stmt.executeUpdate("upsert into test values (2,'Just sample')");
      con.commit();
      //TO-DO Here it will read select * from SYSTEM.TRACING_STATS;
      PreparedStatement statement = con.prepareStatement("select * from test");
      rset = statement.executeQuery();
      while (rset.next()) {
         System.out.println(rset.getString("mycolumn"));
      }
      statement.close();
      con.close();
    } catch (Exception ex) {
      System.out.println(ex);
    }
  }
}
