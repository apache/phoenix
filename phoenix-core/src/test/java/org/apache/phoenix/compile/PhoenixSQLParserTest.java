package org.apache.phoenix.compile;

import org.antlr.runtime.ANTLRInputStream;
import org.antlr.runtime.CommonTokenStream;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.PhoenixSQLLexer;
import org.apache.phoenix.parse.PhoenixSQLParser;
import org.junit.Test;

import java.io.InputStream;
import java.io.StringBufferInputStream;

public class PhoenixSQLParserTest {
    @Test
    public void testConstantInList() throws Exception {
        InputStream stream = new StringBufferInputStream("select aCol from aTable where aCol in (1, 2, 3)");
        ANTLRInputStream inputStream = new ANTLRInputStream(stream);
        PhoenixSQLLexer lexer = new PhoenixSQLLexer(inputStream);
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        PhoenixSQLParser parser = new PhoenixSQLParser(tokenStream);
        parser.setParseNodeFactory(new ParseNodeFactory());
        parser.statement();
    }
}
