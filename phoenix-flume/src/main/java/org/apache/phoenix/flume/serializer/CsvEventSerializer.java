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

import static org.apache.phoenix.flume.FlumeConstants.CSV_DELIMITER;
import static org.apache.phoenix.flume.FlumeConstants.CSV_DELIMITER_DEFAULT;
import static org.apache.phoenix.flume.FlumeConstants.CSV_QUOTE;
import static org.apache.phoenix.flume.FlumeConstants.CSV_QUOTE_DEFAULT;
import static org.apache.phoenix.flume.FlumeConstants.CSV_ESCAPE;
import static org.apache.phoenix.flume.FlumeConstants.CSV_ESCAPE_DEFAULT;
import static org.apache.phoenix.flume.FlumeConstants.CSV_ARRAY_DELIMITER;
import static org.apache.phoenix.flume.FlumeConstants.CSV_ARRAY_DELIMITER_DEFAULT;

import java.io.IOException;
import java.io.StringReader;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.phoenix.schema.types.PDataType;
import org.json.JSONArray;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

public class CsvEventSerializer extends BaseEventSerializer {

	private static final Logger logger = LoggerFactory.getLogger(CsvEventSerializer.class);

	private String csvDelimiter;
	private String csvQuote;
	private String csvEscape;
	private String csvArrayDelimiter;
	private CsvLineParser csvLineParser;

	/**
     * 
     */
	@Override
	public void doConfigure(Context context) {
		csvDelimiter = context.getString(CSV_DELIMITER, CSV_DELIMITER_DEFAULT);
		csvQuote = context.getString(CSV_QUOTE, CSV_QUOTE_DEFAULT);
		csvEscape = context.getString(CSV_ESCAPE, CSV_ESCAPE_DEFAULT);
		csvArrayDelimiter = context.getString(CSV_ARRAY_DELIMITER, CSV_ARRAY_DELIMITER_DEFAULT);
		csvLineParser = new CsvLineParser(csvDelimiter.toCharArray()[0], csvQuote.toCharArray()[0],
				csvEscape.toCharArray()[0]);
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

		boolean wasAutoCommit = connection.getAutoCommit();
		connection.setAutoCommit(false);
		try (PreparedStatement colUpsert = connection.prepareStatement(upsertStatement)) {
			String value = null;
			Integer sqlType = null;
			for (Event event : events) {
				byte[] payloadBytes = event.getBody();
				if (payloadBytes == null || payloadBytes.length == 0) {
					continue;
				}
				String payload = new String(payloadBytes);
				CSVRecord csvRecord = csvLineParser.parse(payload);
				if (colNames.size() != csvRecord.size()) {
					logger.debug("payload data {} doesn't match the fields mapping {} ", payload, colNames);
					continue;
				}
				Map<String, String> data = new HashMap<String, String>();
				for (int i = 0; i < csvRecord.size(); i++) {
					data.put(colNames.get(i), csvRecord.get(i));
				}
				Collection<String> values = data.values();
				if (values.contains(null)) {
					logger.debug("payload data {} doesn't match the fields mapping {} ", payload, colNames);
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
						String arrayJson = Arrays.toString(value.split(csvArrayDelimiter));
						JSONArray jsonArray = new JSONArray(new JSONTokener(arrayJson));
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

	static class CsvLineParser {
		private final CSVFormat csvFormat;

		CsvLineParser(char fieldDelimiter, char quote, char escape) {
			this.csvFormat = CSVFormat.DEFAULT.withIgnoreEmptyLines(true).withDelimiter(fieldDelimiter)
					.withEscape(escape).withQuote(quote);
		}

		public CSVRecord parse(String input) throws IOException {
			CSVParser csvParser = new CSVParser(new StringReader(input), this.csvFormat);
			return Iterables.getFirst(csvParser, null);
		}
	}

}