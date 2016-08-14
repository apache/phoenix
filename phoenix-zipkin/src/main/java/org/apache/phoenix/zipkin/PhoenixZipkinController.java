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
import org.springframework.beans.factory.annotation.*;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.PathVariable;

import java.sql.Connection;
import java.sql.SQLException;
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

  // spring will automatically bind value of property
  @Value("${phoenix.host}")
  private String PHOENIX_HOSTX;

  @Value("${phoenix.port}")
  private String PHOENIX_PORTX;

  @RequestMapping("/api/v1/services")
  public String services() throws JSONException {
    JSONArray servicesOut = new JSONArray();
    String json = "{ services: " + getServices("1") + "}";
    JSONObject obj = new JSONObject(json);
    if (obj.get("services") instanceof JSONObject) {
      return obj.get("services").toString();
    } else {
      JSONArray services = (JSONArray) obj.get("services");
      for (int i = 0; i < services.length(); i++) {
        JSONObject currentObj = services.getJSONObject(i);
        servicesOut.put(currentObj.get("hostname"));
      }
    }
    return servicesOut.toString();
  }

  @RequestMapping("/api/v1/spans")
  public String spans(@RequestParam(value = "serviceName") String hostname) {
    JSONArray spanOut = new JSONArray();
    if (!hostname.equalsIgnoreCase("undefined")) {
      try {
        JSONArray spans = new JSONArray(getSpans("5", hostname));
        for (int i = 0; i < spans.length(); i++) {
          JSONObject obj = spans.getJSONObject(i);
          spanOut.put(obj.get("spanname"));
        }
      } catch (Exception JSONException) {
        return spanOut.toString();
      }
    }
    // return spanOut.toString();
    return spanOut.toString();
  }

  @RequestMapping("/api/v1/traces")
  public String tracesv2(@RequestParam(value = "limit", defaultValue = "10") String limit,
          @RequestParam(value = "serviceName") String hostname,
          @RequestParam(value = "spanName", defaultValue = "all") String spanName,
          @RequestParam(value = "lookback", defaultValue = "0") long lookback,
          @RequestParam(value = "endTs", defaultValue = "0") long endTime,
          @RequestParam(value = "minDuration", defaultValue = "0") long minDuration)
          throws JSONException {
    String jsonarray = "[]";
    jsonarray = getTraces(limit, hostname, spanName, endTime, lookback, minDuration);
    JSONObject element;
    String json = "{ traces: " + jsonarray + "}";
    JSONObject obj = new JSONObject(json);
    JSONArray traces = obj.getJSONArray("traces");
    JSONObject trace = new JSONObject();
    JSONArray zipkinspans = new JSONArray();
    JSONArray array = new JSONArray();
    long previousTraceid = 0;
    for (int i = 0; i < traces.length(); ++i) {
      trace = traces.getJSONObject(i);
      element = new JSONObject();
      long currentTraceid = (long) trace.get("traceid");
      JSONArray annotation = getAnnotation(trace.getString("hostname"), trace.getLong("timestamp"));
      JSONArray binaryannotation = new JSONArray();
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
      }

    }
    String tracesStr = array.toString();
    return tracesStr;
  }

  @RequestMapping("/api/v1/dependencies")
  public String dependancy(@RequestParam long endTs, @RequestParam long lookback)
          throws JSONException {
    String jsonarray = "[]";
    long endTime = endTs;
    long startTime = endTs - lookback;
    jsonarray = getTraceByTime(startTime, endTime);
    JSONArray array = new JSONArray(jsonarray);
    JSONArray zipkindependancy = new JSONArray();

    for (int i = 0; i < array.length(); i++) {
      int count = 0;
      JSONObject trace = array.getJSONObject(i);
      String parent = trace.getString("name");
      if (isParentNonExisting(zipkindependancy, parent)) {
        long parentId = trace.getLong("parent_id");
        String child = "";
        for (int j = 0; j < array.length(); j++) {
          if (parentId == array.getJSONObject(j).getLong("id")) {
            count++;
            child = array.getJSONObject(j).getString("name");
          }
        }
        if (count > 0) {
          JSONObject element = dependancyElement(parent, child, count);
          zipkindependancy.put(element);
        }
      }
    }// parent level loop end
    return zipkindependancy.toString();
  }

  @RequestMapping("/api/v1/trace/{traceId}")
  public String traces(@PathVariable String traceId) throws JSONException {
    String jsonarray = "[]";
    jsonarray = getTrace(traceId);
    JSONArray array = new JSONArray(jsonarray);
    JSONArray zipkinspans = new JSONArray();
    for (int i = 0; i < array.length(); ++i) {
      JSONObject trace = array.getJSONObject(i);
      JSONArray annotation = getAnnotation(trace.getString("hostname"), trace.getLong("timestamp"));
      JSONArray binaryannotation = new JSONArray();
      if (trace.getString("name").contains("VALUES ")) {
        String description = trace.getString("name");
        trace.put("name", description.split("VALUES")[0]);
        binaryannotation = getBinaryAnnotations(trace.getString("hostname"), description);
      }
      trace.put("traceId", trace.getLong("traceid"));
      trace.put("parentId", trace.getLong("parentid"));
      trace.put("duration", (trace.getLong("duration")) * 1000);
      trace.put("annotations", annotation);
      trace.put("binaryAnnotations", binaryannotation);
      trace.remove("traceid");
      trace.remove("parentid");
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

  // get trace by trace id
  protected String getTrace(String traceid) {
    String json = null;
    String sqlQuery = "SELECT trace_id as traceId, description as name,"
            + " start_time as timestamp, span_id as id, parent_id as parentId, hostname, "
            + " ( end_time-start_time ) as duration FROM " + TRACING_TABLE + " WHERE trace_id = "
            + traceid;
    json = getResults(sqlQuery);
    return json;
  }

  // get trace by time
  protected String getTraceByTime(long start, long end) {
    String json = null;
    String limit = "10000";
    String sqlQuery = "SELECT trace_id as traceId, description as name,"
            + " span_id as id, parent_id" + " FROM " + TRACING_TABLE + " WHERE start_time > "
            + start + " AND end_time < " + end + " LIMIT " + limit;
    json = getResults(sqlQuery);
    return json;
  }

  // getting span's annotations
  protected JSONArray getAnnotation(String hostname, long timestamp) throws JSONException {
    JSONArray outJsonarray = new JSONArray();
    JSONObject annotation = new JSONObject();
    JSONObject endpoint = new JSONObject();
    // End point support services and port
    endpoint.put("ipv4", hostname);
    endpoint.put("serviceName", hostname);
    endpoint.put("port", "");
    // Mapping annotation values (sr, ss, cs, cr) when tracing support this
    annotation.put("value", "");
    annotation.put("timestamp", timestamp * 1000);
    annotation.put("endpoint", endpoint);
    outJsonarray.put(annotation);
    return outJsonarray;
  }

  // getting binary annotations
  protected JSONArray getBinaryAnnotations(String hostname, String description)
          throws JSONException {
    String key[] = description.trim().split("VALUES")[0].split("\\(")[1].split("\\)")[0].split(",");
    String value[] = description.trim().split("VALUES")[1].split("\\)")[0].split("\\(")[1]
            .split(",");
    JSONArray annotations = new JSONArray();
    if (key.length == value.length) {
      for (int i = 0; i < key.length; i++) {
        JSONObject annotation = new JSONObject();
        JSONObject endpoint = new JSONObject();
        endpoint.put("ipv4", hostname);
        endpoint.put("serviceName", hostname);
        annotation.put("value", value[i]);
        annotation.put("key", key[i]);
        annotation.put("endpoint", endpoint);
        annotations.put(annotation);
      }
    }
    return annotations;
  }

  protected JSONObject dependancyElement(String parent, String child, int callCount)
          throws JSONException {
    JSONObject element = new JSONObject();
    element.put("parent", parent);
    element.put("child", child);
    element.put("callCount", callCount);
    return element;
  }

  protected boolean isParentNonExisting(JSONArray array, String parent) throws JSONException {
    boolean out = true;
    for (int i = 0; i < array.length(); ++i) {
      if (array.getJSONObject(i).getString("parent").equalsIgnoreCase(parent)) {
        out = false;
      }
    }
    return out;
  }

  // get all traces with limit count, host and time stamp
  protected String getTraces(String limit, String hostname, String spanName, long endTime,
          long lookBack, long minDuration) {
    String json = null;
    String addtionalQuery = "";
    if (limit == null) {
      limit = DEFAULT_LIMIT;
    }
    if (!spanName.equalsIgnoreCase("all")) {
      addtionalQuery = " AND description like '" + spanName + "%'";
    }
    long startTimeStamp = endTime - lookBack;
    long minDurationMilSec = minDuration / 1000;
    String sqlQuery = "SELECT tablex.trace_id as traceid, tablex.description as name,"
            + " tablex.start_time as timestamp, tablex.span_id as id, tablex.parent_id,"
            + " tablex.end_time, tablex.start_time, tablex.hostname  FROM " + TRACING_TABLE
            + " as tablex WHERE hostname = '" + hostname + "' " + " AND end_time < " + endTime
            + " AND start_time > " + startTimeStamp + " AND end_time - start_time > "
            + minDurationMilSec + addtionalQuery
            + " GROUP BY tablex.trace_id, tablex.description, timestamp, id,"
            + " tablex.parent_id, tablex.end_time, tablex.start_time, tablex.hostname" + " LIMIT "
            + limit;
    json = getResults(sqlQuery);
    return json;
  }

  // get all span names
  protected String getSpans(String limit, String hostname) {
    String json = null;
    String strLimit = "";
    if (limit.length() != 0) {
      strLimit = " LIMIT " + limit;
    }
    String sqlQuery = "SELECT distinct REGEXP_SUBSTR(description,'[^(@:\\[-]+') AS SPANNAME"
            + " from " + TRACING_TABLE + " where hostname = '" + hostname + "'" + strLimit;
    json = getResults(sqlQuery);
    return json;
  }

  // get all services
  protected String getServices(String limit) {
    String json = null;
    String strLimit = "";
    if (limit.length() != 0) {
      strLimit = " LIMIT " + limit;
    }
    String sqlQuery = "SELECT distinct hostname FROM " + TRACING_TABLE + strLimit;
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