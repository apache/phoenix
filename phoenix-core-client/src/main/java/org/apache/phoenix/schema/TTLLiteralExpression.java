package org.apache.phoenix.schema;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.hbase.Cell;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.CreateTableStatement;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;

public class TTLLiteralExpression extends TTLExpression {
    private final int ttlValue;

    public TTLLiteralExpression(int ttl) {
        Preconditions.checkArgument(ttl >= 0);
        this.ttlValue = ttl;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TTLLiteralExpression that = (TTLLiteralExpression) o;
        return ttlValue == that.ttlValue;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ttlValue);
    }

    @Override
    public String getTTLExpression() {
        return String.valueOf(ttlValue);
    }

    @Override
    public String toString() {
        return getTTLExpression();
    }

    @Override
    public long getTTLForRow(List<Cell> result) {
        return ttlValue;
    }

    @Override
    public void validateTTLOnCreation(PhoenixConnection conn,
                                      CreateTableStatement create) throws SQLException {

    }

    @Override
    public void validateTTLOnAlter(PTable table) throws SQLException {

    }

    @Override
    public String getTTLForScanAttribute() {
        if (this.equals(TTLExpression.TTL_EXPRESSION_NOT_DEFINED)) {
            return null;
        }
        return getTTLExpression();
    }

    public int getTTLValue() {
        return ttlValue;
    }
}
