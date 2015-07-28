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
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.codehaus.jackson.map.ObjectMapper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

/**
 *
 * Server to show trace information
 *
 *
 * @since 4.4.1
 */

public class TraceServlet extends HttpServlet {

  protected Connection con;

  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    String action = request.getParameter("action");
    String jsonObject = "{}";
    if ("getall".equals(action)) {
      jsonObject = getAll();
    } else {
      jsonObject = "{ key1x: 'value1', key2x: 'value2' }";
    }
    response.setContentType("application/json");
    PrintWriter out = response.getWriter();
    out.print(jsonObject);
    out.flush();

  }

  protected String getAll() {
    String json = null;
    try {
      Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
      // TO-DO Improve config jdbc:phoenix port and the host
      con = DriverManager.getConnection("jdbc:phoenix:localhost:2181");
      EntityFactory nutrientEntityFactory = new EntityFactory(con,
          "select * from test");
      List<Map<String, Object>> nutrients = nutrientEntityFactory
          .findMultiple(new Object[] {});

      ObjectMapper mapper = new ObjectMapper();

      json = mapper.writeValueAsString(nutrients);
    //TO-DO Exception handle needed with error msg
    } catch (Exception e) {
      // throw new ServletException(e);
    } finally {
      if (con != null) {
        try {
          con.close();
        } catch (SQLException e) {
          // throw new ServletException(e);
        }
      }
    }
    return json;
  }

}
