/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.commons.csv;

/**
 * Constants for this package.
 *
 * @version $Id: Constants.java 1509069 2013-08-01 02:04:27Z ggregory $
 */
final class Constants {

    static final char BACKSPACE = '\b';
    static final char COMMA = ',';

    /**
     * Starts a comment, the remainder of the line is the comment.
     */
    static final char COMMENT = '#';

    static final char CR = '\r';
    static final Character DOUBLE_QUOTE_CHAR = Character.valueOf('"');
    static final char BACKSLASH = '\\';
    static final char FF = '\f';
    static final char LF = '\n';
    static final char SP = ' ';
    static final char TAB = '\t';
    static final String EMPTY = "";

    /** The end of stream symbol */
    static final int END_OF_STREAM = -1;

    /** Undefined state for the lookahead char */
    static final int UNDEFINED = -2;

    /** According to RFC 4180, line breaks are delimited by CRLF */
    static final String CRLF = "\r\n";

    /**
     * Unicode line separator.
     */
    static final String LINE_SEPARATOR = "\u2028";

    /**
     * Unicode paragraph separator.
     */
    static final String PARAGRAPH_SEPARATOR = "\u2029";

    /**
     * Unicode next line.
     */
    static final String NEXT_LINE = "\u0085";

}
