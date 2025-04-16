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
package org.apache.phoenix.parse;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.sql.SQLException;
import org.antlr.runtime.ANTLRReaderStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.apache.phoenix.exception.PhoenixParserException;

public class BsonExpressionParser {

  private static final ParseNodeFactory DEFAULT_NODE_FACTORY = new ParseNodeFactory();

  private final PhoenixBsonExpressionParser parser;

  public BsonExpressionParser(String query) {
    this(query, DEFAULT_NODE_FACTORY);
  }

  public BsonExpressionParser(String query, ParseNodeFactory factory) {
    PhoenixBsonExpressionLexer lexer;
    try {
      lexer = new PhoenixBsonExpressionLexer(
        new BsonExpressionParser.CaseInsensitiveReaderStream(new StringReader(query)));
    } catch (IOException e) {
      throw new RuntimeException(e); // Impossible
    }
    CommonTokenStream cts = new CommonTokenStream(lexer);
    parser = new PhoenixBsonExpressionParser(cts);
    parser.setParseNodeFactory(factory);
  }

  /**
   * Parses the input as SQL style query for Document data type.
   * @return ParseNode representing the evaluated expression tree.
   * @throws SQLException If something goes wrong.
   */
  public ParseNode parseExpression() throws SQLException {
    try {
      return parser.expression();
    } catch (RecognitionException e) {
      throw PhoenixParserException.newException(e, parser.getTokenNames());
    } catch (RuntimeException e) {
      if (e.getCause() instanceof SQLException) {
        throw (SQLException) e.getCause();
      }
      throw PhoenixParserException.newException(e, parser.getTokenNames());
    }
  }

  private static class CaseInsensitiveReaderStream extends ANTLRReaderStream {
    CaseInsensitiveReaderStream(Reader script) throws IOException {
      super(script);
    }

    @Override
    public int LA(int i) {
      if (i == 0) {
        return 0; // undefined
      }
      if (i < 0) {
        i++; // e.g., translate LA(-1) to use offset 0
      }

      if ((p + i - 1) >= n) {
        return CharStream.EOF;
      }
      return Character.toLowerCase(data[p + i - 1]);
    }
  }

}
