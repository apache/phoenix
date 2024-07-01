package org.apache.phoenix.schema;

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.parse.CreateTableStatement;

public abstract class TTLExpression {

    public static final TTLExpression TTL_EXPRESSION_FORVER =
            new TTLLiteralExpression(HConstants.FOREVER);
    public static final TTLExpression TTL_EXPRESSION_NOT_DEFINED =
            new TTLLiteralExpression(PhoenixDatabaseMetaData.TTL_NOT_DEFINED);

    public static TTLExpression create(String ttlExpr) {
        if (PhoenixDatabaseMetaData.NONE_TTL.equalsIgnoreCase(ttlExpr)) {
            return TTL_EXPRESSION_NOT_DEFINED;
        } else if (PhoenixDatabaseMetaData.FOREVER_TTL.equalsIgnoreCase(ttlExpr)) {
            return TTL_EXPRESSION_FORVER;
        } else {
            try {
                int ttlValue = Integer.parseInt(ttlExpr);
                return create(ttlValue);
            } catch (NumberFormatException e) {
                return new TTLConditionExpression(ttlExpr);
            }
        }
    }

    public static TTLExpression create (int ttlValue) {
        if (ttlValue == PhoenixDatabaseMetaData.TTL_NOT_DEFINED) {
            return TTL_EXPRESSION_NOT_DEFINED;
        } else if (ttlValue == HConstants.FOREVER) {
            return TTL_EXPRESSION_FORVER;
        } else {
            return new TTLLiteralExpression(ttlValue);
        }
    }

    abstract public String getTTLExpression();

    abstract public long getTTLForRow(List<Cell> result);

    abstract public String toString();

    abstract public void validateTTLOnCreation(PhoenixConnection conn,
                                               CreateTableStatement create) throws SQLException;

    abstract public void validateTTLOnAlter(PTable table) throws SQLException;

    abstract public String getTTLForScanAttribute();

}
