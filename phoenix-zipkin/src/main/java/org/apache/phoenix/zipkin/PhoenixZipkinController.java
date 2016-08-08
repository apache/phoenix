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
package org.apache.phoenix.zipkin;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.PathVariable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

@RestController
public class PhoenixZipkinController {

  private static Connection con;

  protected String DEFAULT_LIMIT = "10";

  protected String DEFAULT_COUNTBY = "hostname";

  protected String LOGIC_AND = "AND";

  protected String LOGIC_OR = "OR";

  protected String TRACING_TABLE = "SYSTEM.TRACING_STATS";

  @RequestMapping("/")
  public String index() {
    return "Greetings from Spring Boot!";
  }

  @RequestMapping("/foo")
  public String foo() throws SQLException {
    Statement stmt = null;
    ResultSet rset = null;
    String rString = "";
    try {
      Connection con = DriverManager.getConnection("jdbc:phoenix:localhost:2181");
      stmt = con.createStatement();

      PreparedStatement statement = con.prepareStatement("select * from SYSTEM.TRACING_STATS");
      rset = statement.executeQuery();
      while (rset.next()) {
        System.out.println(rset.getString("mycolumn"));
        rString += rset.getString("mycolumn");
      }
      statement.close();
      con.close();
    } catch (SQLException e) {
      throw (e);
    }

    return "Greetings from foo!, " + rString;
  }

  @RequestMapping("/api/v1/services")
  public String services() {
    return "[\"phoenix-server\"]";
  }

  @RequestMapping("/api/v1/spans")
  public String spans() {
    return "[]";
  }

  @RequestMapping("/api/v2/traces")
  public String traces() {
    return "[[{\"traceId\":\"0d73ed4802216e0e\",\"name\":\"bootstrap\",\"id\":\"0d73ed4802216e0e\",\"timestamp\":1469940471944000,\"duration\":12054546,\"annotations\":[{\"endpoint\":{\"serviceName\":\"phoenix-server\",\"ipv4\":\"127.0.0.1\",\"port\":9411},\"timestamp\":1469940472204546,\"value\":\"ApplicationStarted\"},{\"endpoint\":{\"serviceName\":\"phoenix-server\",\"ipv4\":\"127.0.0.1\",\"port\":9411},\"timestamp\":1469940472892282,\"value\":\"ApplicationEnvironmentPrepared\"},{\"endpoint\":{\"serviceName\":\"zipkin-server\",\"ipv4\":\"127.0.0.1\",\"port\":9411},\"timestamp\":1469940473470234,\"value\":\"ApplicationPrepared\"},{\"endpoint\":{\"serviceName\":\"zipkin-server\",\"ipv4\":\"127.0.0.1\",\"port\":9411},\"timestamp\":1469940483864223,\"value\":\"ContextRefreshed\"},{\"endpoint\":{\"serviceName\":\"zipkin-server\",\"ipv4\":\"127.0.0.1\",\"port\":9411},\"timestamp\":1469940483993406,\"value\":\"EmbeddedServletContainerInitialized\"},{\"endpoint\":{\"serviceName\":\"zipkin-server\",\"ipv4\":\"127.0.0.1\",\"port\":9411},\"timestamp\":1469940483998543,\"value\":\"ApplicationReady\"}],\"binaryAnnotations\":[{\"key\":\"lc\",\"value\":\"spring-boot\",\"endpoint\":{\"serviceName\":\"zipkin-server\",\"ipv4\":\"127.0.0.1\",\"port\":9411}}]}]]";
  }

  @RequestMapping("/api/v1/traces")
  public String tracesv2(@RequestParam(value = "limit", defaultValue = "10") String limit)
          throws JSONException {

    String jsonarray = "[]";

    jsonarray = getTraces(limit);
    JSONObject element;
    String json = "{ traces: " + jsonarray + "}";
    JSONObject obj = new JSONObject(json);
    JSONArray traces = obj.getJSONArray("traces");
    System.out.println(traces.length());

    JSONObject trace = new JSONObject();
    JSONArray zipkinspans = new JSONArray();
    JSONArray array = new JSONArray();
    long previousTraceid = 0;
    for (int i = 0; i < traces.length(); ++i) {
      trace = traces.getJSONObject(i);
      element = new JSONObject();
      long currentTraceid = (long) trace.get("traceid");
      JSONArray annotation = new JSONArray();
      JSONArray binaryannotation = new JSONArray();

      // System.out.println(trace.toString());
      element.put("traceId", trace.getString("traceid"));
      element.put("name", trace.getString("name"));
      element.put("id", trace.getString("id"));
      element.put("timestamp", trace.getLong("timestamp") * 1000);
      element.put("duration", (trace.getLong("end_time") - trace.getLong("start_time")) * 1000);
      element.put("parentId", trace.getString("parent_id"));
      element.put("annotations", annotation);
      element.put("binaryAnnotations", binaryannotation);
      if (previousTraceid == currentTraceid || i == 0) {
        zipkinspans.put(element);
        previousTraceid = currentTraceid;
        if (i == traces.length() - 1) {
          array.put(zipkinspans);
        }
      } else {
        previousTraceid = currentTraceid;
        array.put(zipkinspans);
        zipkinspans = new JSONArray();
        zipkinspans.put(element);

        System.out.print("zipkinspans Cleared");
      }

    }
    // array.put(zipkinspans);
    String ss = array.toString();
    return ss;
  }

  @RequestMapping("/api/v2/trace/{traceId}")
  @ResponseBody
  public String tracesV2(@PathVariable String traceId) {
    return "[{\"traceId\":\"24baa9cd369ebd80\",\"name\":\"get\",\"id\":\"24baa9cd369ebd80\",\"timestamp\":1469943807711000,\"duration\":16000,\"annotations\":[{\"endpoint\":{\"serviceName\":\"phoenix-server\",\"ipv4\":\"127.0.0.1\",\"port\":9411},\"timestamp\":1469943807711000,\"value\":\"sr\"},{\"endpoint\":{\"serviceName\":\"zipkin-server\",\"ipv4\":\"127.0.0.1\",\"port\":9411},\"timestamp\":1469943807727000,\"value\":\"ss\"}],\"binaryAnnotations\":[{\"key\":\"http.status_code\",\"value\":\"200\",\"endpoint\":{\"serviceName\":\"zipkin-server\",\"ipv4\":\"127.0.0.1\",\"port\":9411}},{\"key\":\"http.url\",\"value\":\"/api/v1/services\",\"endpoint\":{\"serviceName\":\"zipkin-server\",\"ipv4\":\"127.0.0.1\",\"port\":9411}}]}]";
  }

  @RequestMapping("/api/v1/trace/{traceId}")
  public String traces(@PathVariable String traceId) throws JSONException {
    String jsonarray = "[]";
    jsonarray = getTrace(traceId);
    JSONArray array = new JSONArray(jsonarray);
    JSONArray zipkinspans = new JSONArray();
    for (int i = 0; i < array.length(); ++i) {
      JSONObject trace = array.getJSONObject(i);
      JSONArray annotation = new JSONArray();
      JSONArray binaryannotation = new JSONArray();
      trace.put("traceId", trace.getString("traceid"));
      trace.put("duration", (trace.getLong("duration")) * 1000);
      trace.put("annotations", annotation);
      trace.put("binaryAnnotations", binaryannotation);
      zipkinspans.put(trace);
    }

    return zipkinspans.toString();
  }

  // get all trace results with limit count
  protected String getAll(String limit) {
    String json = null;
    if (limit == null) {
      limit = DEFAULT_LIMIT;
    }
    String sqlQuery = "SELECT * FROM " + TRACING_TABLE + " LIMIT " + limit;
    json = getResults(sqlQuery);
    return json;
  }

  // get trace
  protected String getTrace(String traceid) {
    String json = null;
    System.out.print(traceid);
    String sqlQuery = "SELECT trace_id as traceId, description as name,"
            + " start_time as timestamp, span_id as id, parent_id,"
            + " ( end_time-start_time ) as duration FROM " + TRACING_TABLE + " WHERE trace_id = "
            + traceid;
    json = getResults(sqlQuery);
    System.out.print(json);
    return json;
  }

  // get all traces with limit count
  protected String getTraces(String limit) {
    String json = null;
    if (limit == null) {
      limit = DEFAULT_LIMIT;
    }
    String sqlQuery = "SELECT tablex.trace_id as traceid, tablex.description as name,"
            + " tablex.start_time as timestamp, tablex.span_id as id, tablex.parent_id,"
            + " tablex.end_time, tablex.start_time  FROM " + TRACING_TABLE
            + " as tablex GROUP BY tablex.trace_id, tablex.description, timestamp, id,"
            + " tablex.parent_id, tablex.end_time, tablex.start_time  " + "LIMIT " + limit;
    json = getResults(sqlQuery);
    return json;
  }

  // get results with passing sql query
  protected String getResults(String sqlQuery) {
    String json = null;
    if (sqlQuery == null) {
      json = "{error:true,msg:'SQL was null'}";
    } else {
      try {
        con = ConnectionFactory.getConnection();
        EntityFactory nutrientEntityFactory = new EntityFactory(con, sqlQuery);
        List<Map<String, Object>> nutrients = nutrientEntityFactory.findMultiple(new Object[] {});
        System.out.println(nutrients.toString());
        ObjectMapper mapper = new ObjectMapper();
        json = mapper.writeValueAsString(nutrients);
      } catch (Exception e) {
        json = "{error:true,msg:'Serrver Error:" + e.getMessage() + "'}";
      } finally {
        if (con != null) {
          try {
            con.close();
          } catch (SQLException e) {
            json = "{error:true,msg:'SQL Serrver Error:" + e.getMessage() + "'}";
          }
        }
      }
    }
    return json;
  }

}