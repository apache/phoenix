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
import org.apache.phoenix.metrics.MetricInfo;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 *
 * Server to show trace information
 *
 */
public class TraceServlet extends HttpServlet {

  private static final long serialVersionUID = -354285100083055559L;
  private static Connection con;
  protected String DEFAULT_LIMIT = "25";
  protected String DEFAULT_COUNTBY = "hostname";
  protected String LOGIC_AND = "AND";
  protected String LOGIC_OR = "OR";
  protected String TRACING_TABLE = "SYSTEM.TRACING_STATS";



  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    //reading url params
    String action = request.getParameter("action");
    String limit = request.getParameter("limit");
    String traceid = request.getParameter("traceid");
    String parentid = request.getParameter("parentid");
    String jsonObject = "{}";
    if ("getall".equals(action)) {
      jsonObject = getAll(limit);
    } else if ("getCount".equals(action)) {
      jsonObject = getCount("description");
    } else if ("getDistribution".equals(action)) {
      jsonObject = getCount(DEFAULT_COUNTBY);
    } else if ("searchTrace".equals(action)) {
      jsonObject = searchTrace(parentid, traceid, LOGIC_OR);
    } else {
      jsonObject = "{ \"Server\": \"Phoenix Tracing Web App\", \"API version\": 0.1 }";
    }
    //response send as json
    response.setContentType("application/json");
    String output = jsonObject;
    PrintWriter out = response.getWriter();
    out.print(output);
    out.flush();
  }

  //get all trace results with limit count
  protected String getAll(String limit) {
    String json = null;
    if(limit == null) {
      limit = DEFAULT_LIMIT;
    }
    try{
        Long.parseLong(limit);
    } catch (NumberFormatException e) {
    	throw new RuntimeException("The LIMIT passed to the query is not a number.", e);
    }
    String sqlQuery = "SELECT * FROM " + TRACING_TABLE + " LIMIT "+limit;
    json = getResults(sqlQuery);
    return getJson(json);
  }

  //get count on traces can pick on param to count
  protected String getCount(String countby) {
    String json = null;
    if(countby == null) {
      countby = DEFAULT_COUNTBY;
    }
    // Throws exception if the column not present in the trace table.
    MetricInfo.getColumnName(countby.toLowerCase());
    String sqlQuery = "SELECT "+countby+", COUNT(*) AS count FROM " + TRACING_TABLE + " GROUP BY "+countby+" HAVING COUNT(*) > 1 ";
    json = getResults(sqlQuery);
    return json;
  }

  //search the trace over parent id or trace id
  protected String searchTrace(String parentId, String traceId,String logic) {
    String json = null;
    String query = null;
    // Check the parent Id, trace id type or long or not.
    try {
        Long.parseLong(parentId);
        Long.parseLong(traceId);
    } catch (NumberFormatException e) {
    	throw new RuntimeException("The passed parentId/traceId is not a number.", e);
    }
    if(!logic.equals(LOGIC_AND) || !logic.equals(LOGIC_OR)) {
    	throw new RuntimeException("Wrong logical operator passed to the query. Only "+ LOGIC_AND+","+LOGIC_OR+" are allowed.") ;
    }
    if(parentId != null && traceId != null) {
      query = "SELECT * FROM " + TRACING_TABLE + " WHERE parent_id="+parentId+" "+logic+" trace_id="+traceId;
    }else if (parentId != null && traceId == null) {
      query = "SELECT * FROM " + TRACING_TABLE + " WHERE parent_id="+parentId;
    }else if(parentId == null && traceId != null) {
      query = "SELECT * FROM " + TRACING_TABLE + " WHERE trace_id="+traceId;
    }
    json = getResults(query);
    return getJson(json);
  }

  //return json string
  protected String getJson(String json) {
    String output = json.toString().replace("_id\":", "_id\":\"")
        .replace(",\"hostname", "\",\"hostname")
        .replace(",\"parent", "\",\"parent")
        .replace(",\"end", "\",\"end");
    return output;
  }

  //get results with passing sql query
  protected String getResults(String sqlQuery) {
    String json = null;
    if(sqlQuery == null){
      json = "{error:true,msg:'SQL was null'}";
    }else{
    try {
      con = ConnectionFactory.getConnection();
      EntityFactory nutrientEntityFactory = new EntityFactory(con,sqlQuery);
      List<Map<String, Object>> nutrients = nutrientEntityFactory
          .findMultiple();
      ObjectMapper mapper = new ObjectMapper();
      json = mapper.writeValueAsString(nutrients);
    } catch (Exception e) {
      json = "{error:true,msg:'Serrver Error:"+e.getMessage()+"'}";
    } finally {
      if (con != null) {
        try {
          con.close();
        } catch (SQLException e) {
          json = "{error:true,msg:'SQL Serrver Error:"+e.getMessage()+"'}";
        }
      }
    }
    }
    return json;
  }
}
