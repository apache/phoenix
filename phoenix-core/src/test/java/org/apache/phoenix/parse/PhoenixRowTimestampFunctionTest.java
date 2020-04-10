package org.apache.phoenix.parse;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PhoenixRowTimestampFunctionTest {

    @Test
    public void testExpressionWithPhoenixRowTimestamp() throws Exception {
        ParseNode parseNode = SQLParser.parseCondition("(PHOENIX_ROW_TIMESTAMP() = PK2)");
        boolean hasPhoenixRowTimestampParseNode = false;
        for (ParseNode childNode : parseNode.getChildren()) {
            if (childNode.getClass().isAssignableFrom(PhoenixRowTimestampParseNode.class)) {
                assertEquals(0, childNode.getChildren().size());
                hasPhoenixRowTimestampParseNode = true;
            }
        }
        assertTrue(hasPhoenixRowTimestampParseNode);
    }

    @Test
    public void testExpressionWithPhoenixRowTimestampWithParams() throws Exception {
        ParseNode parseNode = SQLParser.parseCondition("(PHOENIX_ROW_TIMESTAMP(COL1) = PK2)");
        for (ParseNode childNode : parseNode.getChildren()) {
            assertFalse("PhoenixRowTimestampFunction does not take any parameters",
                    childNode.getClass().isAssignableFrom(PhoenixRowTimestampParseNode.class));
        }
    }

    @Test
    public void testSelectWithPhoenixRowTimestamp() throws Exception {
        SQLParser parser = new SQLParser("SELECT PHOENIX_ROW_TIMESTAMP() FROM xyz");
        List<AliasedNode> nodes = parser.parseQuery().getSelect();
        assertEquals(1, nodes.size());
        assertTrue("PHOENIX_ROW_TIMESTAMP() should parse to PhoenixRowTimestampParseNode",
                nodes.get(0).getNode().getClass().isAssignableFrom(PhoenixRowTimestampParseNode.class));
        assertEquals(0, nodes.get(0).getNode().getChildren().size());
    }

}
