package org.apache.phoenix.parse;

public class DeleteJarStatement extends MutableStatement {

    private LiteralParseNode jarPath;

    public DeleteJarStatement(LiteralParseNode jarPath) {
        this.jarPath = jarPath;
    }

    @Override
    public int getBindCount() {
        return 0;
    }

    public LiteralParseNode getJarPath() {
        return jarPath;
    }
}
