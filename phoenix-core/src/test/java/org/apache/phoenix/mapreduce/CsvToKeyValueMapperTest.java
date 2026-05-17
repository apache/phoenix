/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;

public class CsvToKeyValueMapperTest {

  @Test
  public void testCsvLineParser() throws IOException {
    CsvToKeyValueMapper.CsvLineParser lineParser =
      new CsvToKeyValueMapper.CsvLineParser(';', '"', '\\');
    CSVRecord parsed = lineParser.parse("one;two");

    assertEquals("one", parsed.get(0));
    assertEquals("two", parsed.get(1));
    assertTrue(parsed.isConsistent());
    assertEquals(1, parsed.getRecordNumber());
  }

  @Test
  public void testCsvLineParserWithQuoting() throws IOException {
    CsvToKeyValueMapper.CsvLineParser lineParser =
      new CsvToKeyValueMapper.CsvLineParser(';', '"', '\\');
    CSVRecord parsed = lineParser.parse("\"\\\"one\";\"\\;two\\\\\"");

    assertEquals("\"one", parsed.get(0));
    assertEquals(";two\\", parsed.get(1));
    assertTrue(parsed.isConsistent());
    assertEquals(1, parsed.getRecordNumber());
  }

  @Test(expected = IOException.class)
  public void testCsvLineParserUnterminatedQuoteThrowsIOException() throws IOException {
    CsvToKeyValueMapper.CsvLineParser lineParser =
      new CsvToKeyValueMapper.CsvLineParser(',', '"', '\\');
    // Unterminated quoted field should throw IOException
    lineParser.parse("1,\"unterminated quote,3");
  }

  @Test(expected = IOException.class)
  public void testCsvLineParserEOFInEncapsulatedToken() throws IOException {
    CsvToKeyValueMapper.CsvLineParser lineParser =
      new CsvToKeyValueMapper.CsvLineParser(',', '"', '\\');
    // This simulates the exact error: "EOF reached before encapsulated token finished"
    lineParser.parse("3,\"Charlie,Sales");
  }

  @Test
  public void testCsvLineParserEmptyLineReturnsNull() throws IOException {
    CsvToKeyValueMapper.CsvLineParser lineParser =
      new CsvToKeyValueMapper.CsvLineParser(',', '"', '\\');
    CSVRecord parsed = lineParser.parse("");
    assertNull(parsed);
  }

  @Test
  public void testCsvLineParserValidRecordAfterBadRecord() throws IOException {
    CsvToKeyValueMapper.CsvLineParser lineParser =
      new CsvToKeyValueMapper.CsvLineParser(',', '"', '\\');

    // Bad record throws
    try {
      lineParser.parse("3,\"Charlie,Sales");
      fail("Expected IOException for unterminated quote");
    } catch (IOException e) {
      // expected
    }

    // Valid record still parses fine (parser is stateless per line)
    CSVRecord parsed = lineParser.parse("4,Diana,Finance");
    assertEquals("4", parsed.get(0));
    assertEquals("Diana", parsed.get(1));
    assertEquals("Finance", parsed.get(2));
  }

  @Test
  public void testCsvLineParserEmbeddedCommaInQuotedField() throws IOException {
    CsvToKeyValueMapper.CsvLineParser lineParser =
      new CsvToKeyValueMapper.CsvLineParser(',', '"', '\\');
    // Valid: comma inside quoted field
    CSVRecord parsed = lineParser.parse("1,\"Sales, Marketing\",HR");
    assertEquals("1", parsed.get(0));
    assertEquals("Sales, Marketing", parsed.get(1));
    assertEquals("HR", parsed.get(2));
  }

  @Test
  public void testCsvLineParserOnlyDelimiters() throws IOException {
    CsvToKeyValueMapper.CsvLineParser lineParser =
      new CsvToKeyValueMapper.CsvLineParser(',', '"', '\\');
    // Valid: three empty fields
    CSVRecord parsed = lineParser.parse(",,");
    assertEquals("", parsed.get(0));
    assertEquals("", parsed.get(1));
    assertEquals("", parsed.get(2));
  }

  @Test(expected = IOException.class)
  public void testCsvLineParserSingleDanglingQuote() throws IOException {
    CsvToKeyValueMapper.CsvLineParser lineParser =
      new CsvToKeyValueMapper.CsvLineParser(',', '"', '\\');
    // Single dangling quote - unterminated encapsulated token
    lineParser.parse("\"");
  }

  @Test(expected = IOException.class)
  public void testCsvLineParserInvalidCharAfterQuotedField() throws IOException {
    CsvToKeyValueMapper.CsvLineParser lineParser =
      new CsvToKeyValueMapper.CsvLineParser(',', '"', '\\');
    // Invalid char between closing quote and delimiter: "Eve,"Bad
    lineParser.parse("5,\"Eve,\"Bad quote handling");
  }
}
