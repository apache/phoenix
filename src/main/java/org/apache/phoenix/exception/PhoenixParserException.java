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
package org.apache.phoenix.exception;

import java.sql.SQLSyntaxErrorException;

import org.antlr.runtime.*;

import org.apache.phoenix.parse.PhoenixSQLParser;


public class PhoenixParserException extends SQLSyntaxErrorException {
    private static final long serialVersionUID = 1L;

    public PhoenixParserException(Exception e, PhoenixSQLParser parser) {
        super(new SQLExceptionInfo.Builder(getErrorCode(e)).setRootCause(e)
                .setMessage(getErrorMessage(e, parser)).build().toString(),
                getErrorCode(e).getSQLState(), getErrorCode(e).getErrorCode(), e);
    }

    public static String getLine(RecognitionException e) {
        return Integer.toString(e.token.getLine());
    }

    public static String getColumn(RecognitionException e) {
        return Integer.toString(e.token.getCharPositionInLine() + 1);
    }

    public static String getTokenLocation(RecognitionException e) {
        return "line " + getLine(e) + ", column " + getColumn(e) + ".";
    }

    public static String getErrorMessage(Exception e, PhoenixSQLParser parser) {
        String[] tokenNames = parser.getTokenNames();
        String msg;
        if (e instanceof MissingTokenException) {
            MissingTokenException mte = (MissingTokenException)e;
            String tokenName;
            if (mte.expecting== Token.EOF) {
                tokenName = "EOF";
            } else {
                tokenName = tokenNames[mte.expecting];
            }
            msg = "Missing \""+ tokenName +"\" at "+ getTokenLocation(mte);
        } else if (e instanceof UnwantedTokenException) {
            UnwantedTokenException ute = (UnwantedTokenException)e;
            String tokenName;
            if (ute.expecting== Token.EOF) {
                tokenName = "EOF";
            } else {
                tokenName = tokenNames[ute.expecting];
            }
            msg = "Unexpected input. Expecting \"" + tokenName + "\", got \"" + ute.getUnexpectedToken().getText() 
                    + "\" at " + getTokenLocation(ute);
        } else if (e instanceof MismatchedTokenException) {
            MismatchedTokenException mte = (MismatchedTokenException)e;
            String tokenName;
            if (mte.expecting== Token.EOF) {
                tokenName = "EOF";
            } else {
                tokenName = tokenNames[mte.expecting];
            }
            msg = "Mismatched input. Expecting \"" + tokenName + "\", got \"" + mte.token.getText()
                    + "\" at " + getTokenLocation(mte);
        } else if (e instanceof RecognitionException){
            RecognitionException re = (RecognitionException) e;
            msg = "Encountered \"" + re.token.getText() + "\" at " + getTokenLocation(re);
        } else if (e instanceof UnknownFunctionException) {
            UnknownFunctionException ufe = (UnknownFunctionException) e;
            msg = "Unknown function: \"" + ufe.getFuncName() + "\".";
        } else {
            msg = e.getMessage();
        }
        return msg;
    }

    public static SQLExceptionCode getErrorCode(Exception e) {
        if (e instanceof MissingTokenException) {
            return SQLExceptionCode.MISSING_TOKEN;
        } else if (e instanceof UnwantedTokenException) {
            return SQLExceptionCode.UNWANTED_TOKEN;
        } else if (e instanceof MismatchedTokenException) {
            return SQLExceptionCode.MISMATCHED_TOKEN;
        } else if (e instanceof UnknownFunctionException) {
            return SQLExceptionCode.UNKNOWN_FUNCTION;
        } else {
            return SQLExceptionCode.PARSER_ERROR;
        }
    }
}
