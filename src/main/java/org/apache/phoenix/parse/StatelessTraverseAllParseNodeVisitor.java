package org.apache.phoenix.parse;

import java.sql.SQLException;
import java.util.List;

public class StatelessTraverseAllParseNodeVisitor extends TraverseAllParseNodeVisitor<Void> {
    @Override
    public Void visitLeave(LikeParseNode node, List<Void> l) throws SQLException {
        return null;
    }

    @Override
    public Void visitLeave(AndParseNode node, List<Void> l) throws SQLException {
        return null;
    }

    @Override
    public Void visitLeave(OrParseNode node, List<Void> l) throws SQLException {
        return null;
    }

    @Override
    public Void visitLeave(FunctionParseNode node, List<Void> l) throws SQLException {
        return null;
    }

    @Override
    public Void visitLeave(ComparisonParseNode node, List<Void> l) throws SQLException {
        return null;
    }

    @Override
    public Void visitLeave(CaseParseNode node, List<Void> l) throws SQLException {
        return null;
    }

    @Override
    public Void visitLeave(AddParseNode node, List<Void> l) throws SQLException {
        return null;
    }

    @Override
    public Void visitLeave(MultiplyParseNode node, List<Void> l) throws SQLException {
        return null;
    }

    @Override
    public Void visitLeave(DivideParseNode node, List<Void> l) throws SQLException {
        return null;
    }

    @Override
    public Void visitLeave(SubtractParseNode node, List<Void> l) throws SQLException {
        return null;
    }

    @Override
    public Void visitLeave(NotParseNode node, List<Void> l) throws SQLException {
        return null;
    }
    
    @Override
    public Void visitLeave(CastParseNode node, List<Void> l) throws SQLException {
        return null;
    }
    
    @Override
    public Void visitLeave(InListParseNode node, List<Void> l) throws SQLException {
        return null;
    }

    @Override
    public Void visitLeave(StringConcatParseNode node, List<Void> l) throws SQLException {
        return null;
    }

    @Override
    public Void visitLeave(BetweenParseNode node, List<Void> l) throws SQLException {
        return null;
    }

    @Override
    public Void visitLeave(RowValueConstructorParseNode node, List<Void> l) throws SQLException {
        return null;
    }
}
