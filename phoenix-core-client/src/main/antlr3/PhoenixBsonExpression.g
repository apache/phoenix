/**
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
grammar PhoenixBsonExpression;

tokens
{
    NOT='not';
    AND='and';
    OR='or';
    NULL='null';
    TRUE='true';
    FALSE='false';
    IN='in';
    IS='is';
    BETWEEN='between';
    ATTR = 'attribute_exists';
    ATTR_NOT = 'attribute_not_exists';
    FIELD = 'field_exists';
    FIELD_NOT = 'field_not_exists';
    BEGINS_WITH = 'begins_with';
    CONTAINS = 'contains';
    FIELD_TYPE = 'field_type';
    ATTR_TYPE = 'attribute_type';
}

@parser::header {
/**
 *
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

///CLOVER:OFF
import org.apache.hadoop.hbase.CompareOperator;
import java.lang.Boolean;
import org.apache.phoenix.util.SchemaUtil;
}

@lexer::header {
/**
 * Copyright 2010 The Apache Software Foundation
 *
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
///CLOVER:OFF
}

// --------------------------------------
// The Parser

@parser::members
{

    private ParseNodeFactory factory;

    public void setParseNodeFactory(ParseNodeFactory factory) {
        this.factory = factory;
    }

    public int line(Token t) {
        return t.getLine();
    }

    public int column(Token t) {
        return t.getCharPositionInLine() + 1;
    }

    protected Object recoverFromMismatchedToken(IntStream input, int ttype, BitSet follow)
        throws RecognitionException {
        RecognitionException e = null;
        // if next token is what we are looking for then "delete" this token
        if (mismatchIsUnwantedToken(input, ttype)) {
            e = new UnwantedTokenException(ttype, input);
        } else if (mismatchIsMissingToken(input, follow)) {
            Object inserted = getMissingSymbol(input, e, ttype, follow);
            e = new MissingTokenException(ttype, input, inserted);
        } else {
            e = new MismatchedTokenException(ttype, input);
        }
        throw e;
    }

    public Object recoverFromMismatchedSet(IntStream input, RecognitionException e, BitSet follow)
        throws RecognitionException
    {
        throw e;
    }

    @Override
    public String getErrorMessage(RecognitionException e, String[] tokenNames) {
        if (e instanceof MismatchedTokenException) {
            MismatchedTokenException mte = (MismatchedTokenException)e;
            String txt = mte.token.getText();
            String p = mte.token.getType() == -1 ? "EOF" : PARAPHRASE[mte.token.getType()];
            String expecting = (mte.expecting < PARAPHRASE.length && mte.expecting >= 0) ? PARAPHRASE[mte.expecting] : null;
            if (expecting == null) {
                return "unexpected token (" + line(mte.token) + "," + column(mte.token) + "): " + (txt != null ? txt : p);
            } else {
                return "expecting " + expecting +
                    ", found '" + (txt != null ? txt : p) + "'";
            }
        } else if (e instanceof NoViableAltException) {
            //NoViableAltException nvae = (NoViableAltException)e;
            return "unexpected token: (" + line(e.token) + "," + column(e.token) + ")" + getTokenErrorDisplay(e.token);
        }
        return super.getErrorMessage(e, tokenNames);
     }

    public String getTokenErrorDisplay(int t) {
        String ret = PARAPHRASE[t];
        if (ret == null) ret = "<UNKNOWN>";
        return ret;
    }


    private String[] PARAPHRASE = new String[getTokenNames().length];
    {
        PARAPHRASE[POSINTEGER] = "a number";
        PARAPHRASE[EQ] = "an equals sign";
        PARAPHRASE[LT] = "a left angle bracket";
        PARAPHRASE[GT] = "a right angle bracket";
        PARAPHRASE[COMMA] = "a comma";
        PARAPHRASE[LPAREN] = "a left parentheses";
        PARAPHRASE[RPAREN] = "a right parentheses";
        PARAPHRASE[SEMICOLON] = "a semi-colon";
        PARAPHRASE[COLON] = "a colon";
        PARAPHRASE[LSQUARE] = "left square bracket";
        PARAPHRASE[RSQUARE] = "right square bracket";
        PARAPHRASE[LCURLY] = "left curly bracket";
        PARAPHRASE[RCURLY] = "right curly bracket";
        PARAPHRASE[AT] = "at";
        PARAPHRASE[MINUS] = "a subtraction";
        PARAPHRASE[TILDE] = "a tilde";
        PARAPHRASE[PLUS] = "an addition";
        PARAPHRASE[ASTERISK] = "an asterisk";
        PARAPHRASE[DIVIDE] = "a division";
        PARAPHRASE[LETTER] = "an ansi letter";
        PARAPHRASE[POSINTEGER] = "a positive integer";
        PARAPHRASE[DIGIT] = "a number from 0 to 9";
    }
}

@rulecatch {
    catch (RecognitionException re) {
        throw re;
    }
}

@lexer::members {

}

string_literal returns [String ret]
    :   s=STRING_LITERAL { ret = SchemaUtil.normalizeLiteral(factory.literal(s.getText())); }
    ;

expression returns [ParseNode ret]
    :   e=or_expression { $ret = e; }
    ;

or_expression returns [ParseNode ret]
@init{List<ParseNode> l = new ArrayList<ParseNode>(4); }
    :   i=and_expression {l.add(i);} (OR i=and_expression {l.add(i);})* { $ret = l.size() == 1 ? l.get(0) : factory.or(l); }
    ;

and_expression returns [ParseNode ret]
@init{List<ParseNode> l = new ArrayList<ParseNode>(4); }
    :   i=not_expression {l.add(i);} (AND i=not_expression {l.add(i);})* { $ret = l.size() == 1 ? l.get(0) : factory.and(l); }
    ;

not_expression returns [ParseNode ret]
    :   (NOT? boolean_expression ) => n=NOT? e=boolean_expression { $ret = n == null ? e : factory.not(e); }
    |   n=NOT? LPAREN e=expression RPAREN { $ret = n == null ? e : factory.not(e); }
    ;

comparison_op returns [CompareOperator ret]
	: EQ { $ret = CompareOperator.EQUAL; }
	| LT { $ret = CompareOperator.LESS; }
	| GT { $ret = CompareOperator.GREATER; }
	| LT EQ { $ret = CompareOperator.LESS_OR_EQUAL; }
	| GT EQ { $ret = CompareOperator.GREATER_OR_EQUAL; }
	| (NOEQ1 | NOEQ2) { $ret = CompareOperator.NOT_EQUAL; }
	;

boolean_expression returns [ParseNode ret]
    :   l=value_expression ((op=comparison_op (r=value_expression) {$ret = factory.comparison(op,l,r); } )
                  |  (IS n=NOT? NULL {$ret = factory.isNull(l,n!=null); } )
                  |  ( n=NOT? (
                               (BETWEEN r1=value_expression AND r2=value_expression {$ret = factory.between(l,r1,r2,n!=null); } )
                      |        (IN (LPAREN v=one_or_more_expressions RPAREN {List<ParseNode> il = new ArrayList<ParseNode>(v.size() + 1); il.add(l); il.addAll(v); $ret = factory.inList(il,n!=null);}))
                      ))
                  |  { $ret = l; } )
        |   (ATTR | FIELD) ( LPAREN t=literal RPAREN {$ret = factory.documentFieldExists(t, true); } )
        |   (ATTR_NOT | FIELD_NOT) ( LPAREN t=literal RPAREN {$ret = factory.documentFieldExists(t, false); } )
        |   BEGINS_WITH ( LPAREN l=value_expression COMMA r=value_expression RPAREN
                {$ret = factory.documentFieldBeginsWith(l, r); } )
        |   CONTAINS ( LPAREN l=value_expression COMMA r=value_expression RPAREN
                {$ret = factory.documentFieldContains(l, r); } )
        |   (FIELD_TYPE | ATTR_TYPE) ( LPAREN l=value_expression COMMA r=value_expression RPAREN
                {$ret = factory.documentFieldType(l, r); } )
    ;

value_expression returns [ParseNode ret]
    :   e=term  { $ret = e; }
    ;

term returns [ParseNode ret]
    :   e=literal_or_bind { $ret = e; }
    |   LPAREN l=one_or_more_expressions RPAREN
    	{
    		if(l.size() == 1) {
    			$ret = l.get(0);
    		}
    		else {
    			$ret = factory.rowValueConstructor(l);
    		}
    	}
    ;

one_or_more_expressions returns [List<ParseNode> ret]
@init{ret = new ArrayList<ParseNode>(); }
    :  e = expression {$ret.add(e);}  (COMMA e = expression {$ret.add(e);} )*
;

zero_or_more_expressions returns [List<ParseNode> ret]
@init{ret = new ArrayList<ParseNode>(); }
    :  (v = expression {$ret.add(v);})?  (COMMA v = expression {$ret.add(v);} )*
;

value_expression_list returns [List<ParseNode> ret]
@init{ret = new ArrayList<ParseNode>(); }
    :  LPAREN e = value_expression {$ret.add(e);}  (COMMA e = value_expression {$ret.add(e);} )* RPAREN
;

literal_or_bind returns [ParseNode ret]
    :   e=literal { $ret = e; }
    ;

literal returns [LiteralParseNode ret]
    :
        f=BIND_VALUE {
            ret = factory.literal(f.getText());
        }
    |   i=DOCUMENT_FIELD {
            ret = factory.literal(i.getText());
        }
    |   j=HASH_DOCUMENT_FIELD {
            ret = factory.literal(j.getText());
        }
    |   s=STRING_LITERAL {
            ret = factory.literal(s.getText());
        }
    |   n=POSINTEGER {
            ret = factory.wholeNumber(n.getText());
        }
    |   d=DECIMAL  {
            ret = factory.realNumber(d.getText());
        }
    |   dbl=DOUBLE  {
            ret = factory.literal(Double.valueOf(dbl.getText()));
        }
    |   NULL {ret = factory.literal(null);}
    |   TRUE {ret = factory.literal(Boolean.TRUE);}
    |   FALSE {ret = factory.literal(Boolean.FALSE);}
    ;

// --------------------------------------
// The Lexer

DECIMAL
	:	POSINTEGER? '.' POSINTEGER
	;

DOUBLE
    :   '.' POSINTEGER Exponent
    |   POSINTEGER '.' Exponent
    |   POSINTEGER ('.' (POSINTEGER (Exponent)?)? | Exponent)
    ;

Exponent
    :    ('e' | 'E') ( PLUS | MINUS )? POSINTEGER
    ;

DOUBLE_QUOTE
    :   '"'
    ;

EQ
    :   '='
    ;

LT
    :   '<'
    ;

GT
    :   '>'
    ;

DOUBLE_EQ
    :   '=''='
    ;

NOEQ1
    :   '!''='
    ;

NOEQ2
    :   '<''>'
    ;

CONCAT
    :   '|''|'
    ;

COMMA
    :   ','
    ;

LPAREN
    :   '('
    ;

RPAREN
    :   ')'
    ;

SEMICOLON
    :   ';'
    ;

COLON
    :   ':'
    ;

QUESTION
    :   '?'
    ;

LSQUARE
    :   '['
    ;

RSQUARE
    :   ']'
    ;

LCURLY
    :   '{'
    ;

RCURLY
    :   '}'
    ;

AT
    :   '@'
    ;

TILDE
    :   '~'
    ;

PLUS
    :   '+'
    ;

MINUS
    :   '-'
    ;

ASTERISK
    :   '*'
    ;

DIVIDE
    :   '/'
    ;

DOT
    :   '.'
    ;

PERCENT
    :   '%'
    ;

UNDERSCORE
    :   '_'
    ;

HASH
    :   '#'
    ;


BIND_VALUE
@init{ StringBuilder sb = new StringBuilder(); }
    :   ( COLON { sb.append(":"); } | '$' { sb.append("$"); } )
    ( t=BIND_VALUE_CHARS { sb.append(t.getText()); } )+
    { setText(sb.toString()); }
    ;

HASH_DOCUMENT_FIELD
@init{ StringBuilder sb = new StringBuilder(); }
    :   ( a=HASH { sb.append(a.getText()); } )
        ( h=HASH { sb.append(h.getText()); }
        | t=DOCUMENT_FIELD { sb.append(t.getText()); }
        )+
    { setText(sb.toString()); }
    ;

DOCUMENT_FIELD
@init{ StringBuilder sb = new StringBuilder(); }
    :   (v1=LETTER { sb.append(v1.getText()); }
        | v2=DIGIT { sb.append(v2.getText()); }
        | v3=UNDERSCORE { sb.append(v3.getText()); }
        | v4=LSQUARE { sb.append(v4.getText()); }
        | v5=RSQUARE { sb.append(v5.getText()); }
        | v6=DOT { sb.append(v6.getText()); }
        | v8=MINUS { sb.append(v8.getText()); }
        )+
        { setText(sb.toString()); }
    ;

fragment
BIND_VALUE_CHARS
    :   (LETTER | DIGIT | '_' | '-')
    ;


// A Letter is a lower or upper case ascii character.
fragment
LETTER
    :    'a'..'z'
    |    'A'..'Z'
    ;

fragment
POSINTEGER
    :   DIGIT+
    ;

fragment
DIGIT
    :    '0'..'9'
    ;

// string literals
STRING_LITERAL
@init{ StringBuilder sb = new StringBuilder(); }
    :   '\''
    ( t=CHAR { sb.append(t.getText()); }
    | t=CHAR_ESC { sb.append(getText()); }
    )* '\'' { setText(sb.toString()); }
    ;

fragment
CHAR
    :   ( ~('\'' | '\\') )
    ;

// escape sequence inside a string literal
fragment
CHAR_ESC
    :   '\\'
        ( 'n'   { setText("\n"); }
        | 'r'   { setText("\r"); }
        | 't'   { setText("\t"); }
        | 'b'   { setText("\b"); }
        | 'f'   { setText("\f"); }
        | '\"'  { setText("\""); }
        | '\''  { setText("\'"); }
        | '\\'  { setText("\\"); }
        | '_'   { setText("\\_"); }
        | '%'   { setText("\\\%"); }
        |       { setText("\\"); }
        )
    |   '\'\''  { setText("\'"); }
    ;

WS
    :   ( ' ' | '\t' | '\u2002' ) { $channel=HIDDEN; }
    ;

EOL
    :  ('\r' | '\n')
    { skip(); }
    ;
