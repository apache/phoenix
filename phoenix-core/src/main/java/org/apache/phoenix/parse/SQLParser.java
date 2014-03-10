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
package org.apache.phoenix.parse;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import org.antlr.runtime.ANTLRReaderStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.apache.phoenix.exception.PhoenixParserException;

/**
 * 
 * SQL Parser for Phoenix
 *
 * 
 * @since 0.1
 */
public class SQLParser {
    private static final ParseNodeFactory DEFAULT_NODE_FACTORY = new ParseNodeFactory();

    private final PhoenixSQLParser parser;

    public static ParseNode parseCondition(String expression) throws SQLException {
        if (expression == null) return null;
        SQLParser parser = new SQLParser(expression);
        return parser.parseExpression();
    }
    
    public SQLParser(String query) {
        this(query,DEFAULT_NODE_FACTORY);
    }

    public SQLParser(String query, ParseNodeFactory factory) {
        PhoenixSQLLexer lexer;
        try {
            lexer = new PhoenixSQLLexer(new CaseInsensitiveReaderStream(new StringReader(query)));
        } catch (IOException e) {
            throw new RuntimeException(e); // Impossible
        }
        CommonTokenStream cts = new CommonTokenStream(lexer);
        parser = new PhoenixSQLParser(cts);
        parser.setParseNodeFactory(factory);
    }

    public SQLParser(Reader queryReader, ParseNodeFactory factory) throws IOException {
        PhoenixSQLLexer lexer = new PhoenixSQLLexer(new CaseInsensitiveReaderStream(queryReader));
        CommonTokenStream cts = new CommonTokenStream(lexer);
        parser = new PhoenixSQLParser(cts);
        parser.setParseNodeFactory(factory);
    }

    public SQLParser(Reader queryReader) throws IOException {
        PhoenixSQLLexer lexer = new PhoenixSQLLexer(new CaseInsensitiveReaderStream(queryReader));
        CommonTokenStream cts = new CommonTokenStream(lexer);
        parser = new PhoenixSQLParser(cts);
        parser.setParseNodeFactory(DEFAULT_NODE_FACTORY);
    }

    /**
     * Parses the input as a series of semicolon-terminated SQL statements.
     * @throws SQLException 
     */
    public BindableStatement nextStatement(ParseNodeFactory factory) throws SQLException {
        try {
            parser.resetBindCount();
            parser.setParseNodeFactory(factory);
            BindableStatement statement = parser.nextStatement();
            return statement;
        } catch (RecognitionException e) {
            throw PhoenixParserException.newException(e, parser.getTokenNames());
        } catch (UnsupportedOperationException e) {
            throw new SQLFeatureNotSupportedException(e);
        } catch (RuntimeException e) {
            if (e.getCause() instanceof SQLException) {
                throw (SQLException) e.getCause();
            }
            throw PhoenixParserException.newException(e, parser.getTokenNames());
        }
    }

    /**
     * Parses the input as a SQL select or upsert statement.
     * @throws SQLException 
     */
    public BindableStatement parseStatement() throws SQLException {
        try {
            BindableStatement statement = parser.statement();
            return statement;
        } catch (RecognitionException e) {
            throw PhoenixParserException.newException(e, parser.getTokenNames());
        } catch (UnsupportedOperationException e) {
            throw new SQLFeatureNotSupportedException(e);
        } catch (RuntimeException e) {
            if (e.getCause() instanceof SQLException) {
                throw (SQLException) e.getCause();
            }
            throw PhoenixParserException.newException(e, parser.getTokenNames());
        }
    }

    /**
     * Parses the input as a SQL select statement.
     * Used only in tests
     * @throws SQLException 
     */
    public SelectStatement parseQuery() throws SQLException {
        try {
            SelectStatement statement = parser.query();
            return statement;
        } catch (RecognitionException e) {
            throw PhoenixParserException.newException(e, parser.getTokenNames());
        } catch (RuntimeException e) {
            if (e.getCause() instanceof SQLException) {
                throw (SQLException) e.getCause();
            }
            throw PhoenixParserException.newException(e, parser.getTokenNames());
        }
    }

    /**
     * Parses the input as a SQL select statement.
     * Used only in tests
     * @throws SQLException 
     */
    public ParseNode parseExpression() throws SQLException {
        try {
            ParseNode node = parser.expression();
            return node;
        } catch (RecognitionException e) {
            throw PhoenixParserException.newException(e, parser.getTokenNames());
        } catch (RuntimeException e) {
            if (e.getCause() instanceof SQLException) {
                throw (SQLException) e.getCause();
            }
            throw PhoenixParserException.newException(e, parser.getTokenNames());
        }
    }

    /**
     * Parses the input as a SQL literal
     * @throws SQLException 
     */
    public LiteralParseNode parseLiteral() throws SQLException {
        try {
            LiteralParseNode literalNode = parser.literal();
            return literalNode;
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
            if (i == 0) { return 0; // undefined
            }
            if (i < 0) {
                i++; // e.g., translate LA(-1) to use offset 0
            }

            if ((p + i - 1) >= n) { return CharStream.EOF; }
            return Character.toLowerCase(data[p + i - 1]);
        }
    }
}