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
package org.apache.phoenix.flume.serializer;

import static org.apache.phoenix.flume.FlumeConstants.JSON_DEFAULT;
import static org.apache.phoenix.flume.FlumeConstants.CONFIG_COLUMNS_MAPPING;
import static org.apache.phoenix.flume.FlumeConstants.CONFIG_PARTIAL_SCHEMA;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.phoenix.schema.types.PDataType;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.spi.json.JsonOrgJsonProvider;
import com.jayway.jsonpath.spi.mapper.JsonOrgMappingProvider;

public class JsonEventSerializer extends BaseEventSerializer {

	private static final Logger logger = LoggerFactory.getLogger(JsonEventSerializer.class);

	private JSONObject jsonSchema;
	private boolean isProperMapping;
	private boolean partialSchema;

	/**
     * 
     */
	@Override
	public void doConfigure(Context context) {
		final String jsonData = context.getString(CONFIG_COLUMNS_MAPPING, JSON_DEFAULT);
		try {
			jsonSchema = new JSONObject(jsonData);
			if (jsonSchema.length() == 0) {
				for (String colName : colNames) {
					jsonSchema.put(colName, colName);
				}
				isProperMapping = true;
			} else {
				Iterator<String> keys = jsonSchema.keys();
				List<String> keylist = new ArrayList<String>();
				while (keys.hasNext()) {
					keylist.add(keys.next());
				}
				isProperMapping = CollectionUtils.isEqualCollection(keylist, colNames);
			}
		} catch (JSONException e) {
			e.printStackTrace();
			logger.debug("json mapping not proper, verify the data {} ", jsonData);
		}
		partialSchema = context.getBoolean(CONFIG_PARTIAL_SCHEMA, false);
	}

	/**
     * 
     */
	@Override
	public void doInitialize() throws SQLException {
		// NO-OP
	}

	@Override
	public void upsertEvents(List<Event> events) throws SQLException {
		Preconditions.checkNotNull(events);
		Preconditions.checkNotNull(connection);
		Preconditions.checkNotNull(this.upsertStatement);
		Preconditions.checkArgument(isProperMapping, "Please verify fields mapping is not properly done..");

		boolean wasAutoCommit = connection.getAutoCommit();
		connection.setAutoCommit(false);
		try (PreparedStatement colUpsert = connection.prepareStatement(upsertStatement)) {
			String value = null;
			Integer sqlType = null;
			JSONObject inputJson = new JSONObject();
			for (Event event : events) {
				byte[] payloadBytes = event.getBody();
				if (payloadBytes == null || payloadBytes.length == 0) {
					continue;
				}
				String payload = new String(payloadBytes);

				try {
					inputJson = new JSONObject(payload);
				} catch (Exception e) {
					logger.debug("payload is not proper json");
					continue;
				}

				Map<String, String> data = new HashMap<String, String>();
				for (String colName : colNames) {
					String pattern = colName;
					if (jsonSchema.has(colName)) {
						Object obj = jsonSchema.opt(colName);
						if (null != obj) {
							pattern = obj.toString();
						}
					}
					pattern = "$." + pattern;
					value = getPatternData(inputJson, pattern);

					// if field mapping data is null then look for column data
					if (null == value && partialSchema) {
						pattern = "$." + colName;
						value = getPatternData(inputJson, pattern);
					}

					data.put(colName, value);
				}

				Collection<String> values = data.values();
				if (values.contains(null)) {
					logger.debug("payload data {} doesn't match the fields mapping {} ", inputJson, jsonSchema);
					continue;
				}

				int index = 1;
				int offset = 0;
				for (int i = 0; i < colNames.size(); i++, offset++) {
					if (columnMetadata[offset] == null) {
						continue;
					}
					String colName = colNames.get(i);
					value = data.get(colName);
					sqlType = columnMetadata[offset].getSqlType();
					PDataType pDataType = PDataType.fromTypeId(sqlType);
					Object upsertValue;
					if (pDataType.isArrayType()) {
						JSONArray jsonArray = new JSONArray(new JSONTokener(value));
						Object[] vals = new Object[jsonArray.length()];
						for (int x = 0; x < jsonArray.length(); x++) {
							vals[x] = jsonArray.get(x);
						}
						String baseTypeSqlName = PDataType.arrayBaseType(pDataType).getSqlTypeName();
						Array array = connection.createArrayOf(baseTypeSqlName, vals);
						upsertValue = pDataType.toObject(array, pDataType);
					} else {
						upsertValue = pDataType.toObject(value);
					}
					if (upsertValue != null) {
						colUpsert.setObject(index++, upsertValue, sqlType);
					} else {
						colUpsert.setNull(index++, sqlType);
					}
				}

				// add headers if necessary
				Map<String, String> headerValues = event.getHeaders();
				for (int i = 0; i < headers.size(); i++, offset++) {
					String headerName = headers.get(i);
					String headerValue = headerValues.get(headerName);
					sqlType = columnMetadata[offset].getSqlType();
					Object upsertValue = PDataType.fromTypeId(sqlType).toObject(headerValue);
					if (upsertValue != null) {
						colUpsert.setObject(index++, upsertValue, sqlType);
					} else {
						colUpsert.setNull(index++, sqlType);
					}
				}

				if (autoGenerateKey) {
					sqlType = columnMetadata[offset].getSqlType();
					String generatedRowValue = this.keyGenerator.generate();
					Object rowkeyValue = PDataType.fromTypeId(sqlType).toObject(generatedRowValue);
					colUpsert.setObject(index++, rowkeyValue, sqlType);
				}
				colUpsert.execute();
			}
			connection.commit();
		} catch (Exception ex) {
			logger.error("An error {} occurred during persisting the event ", ex.getMessage());
			throw new SQLException(ex.getMessage());
		} finally {
			if (wasAutoCommit) {
				connection.setAutoCommit(true);
			}
		}

	}

	private String getPatternData(JSONObject json, String pattern) {
		Configuration JSON_ORG_CONFIGURATION = Configuration.builder().mappingProvider(new JsonOrgMappingProvider())
				.jsonProvider(new JsonOrgJsonProvider()).build();
		String value;
		try {
			Object object = JsonPath.using(JSON_ORG_CONFIGURATION).parse(json).read(pattern);
			value = object.toString();
		} catch (Exception e) {
			value = null;
		}
		return value;
	}

}