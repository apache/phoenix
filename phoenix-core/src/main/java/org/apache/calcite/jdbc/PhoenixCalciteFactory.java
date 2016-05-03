package org.apache.calcite.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.sql.NClob;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaDatabaseMetaData;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.UnregisteredDriver;
import org.apache.calcite.jdbc.CalciteConnectionImpl;
import org.apache.calcite.jdbc.CalciteFactory;
import org.apache.calcite.jdbc.Driver;
import org.apache.phoenix.calcite.PhoenixSchema;

public class PhoenixCalciteFactory extends CalciteFactory {
    
    public PhoenixCalciteFactory() {
        this(4, 1);
    }

    protected PhoenixCalciteFactory(int major, int minor) {
        super(major, minor);
    }

    public AvaticaConnection newConnection(UnregisteredDriver driver,
        AvaticaFactory factory, String url, Properties info,
        CalciteSchema rootSchema, JavaTypeFactory typeFactory) {
        return new PhoenixCalciteConnection(
                (Driver) driver, factory, url, info, rootSchema, typeFactory);
    }

    @Override
    public AvaticaDatabaseMetaData newDatabaseMetaData(
            AvaticaConnection connection) {
        return new PhoenixCalciteDatabaseMetaData(
                (PhoenixCalciteConnection) connection);
    }

    @Override
    public AvaticaStatement newStatement(AvaticaConnection connection,
            StatementHandle h, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        return new PhoenixCalciteStatement((PhoenixCalciteConnection) connection, 
                h, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public AvaticaPreparedStatement newPreparedStatement(
            AvaticaConnection connection, StatementHandle h,
            Signature signature, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        return new PhoenixCalcitePreparedStatement(
                (PhoenixCalciteConnection) connection, h,
                (CalcitePrepare.CalciteSignature) signature,
                resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public CalciteResultSet newResultSet(AvaticaStatement statement, QueryState state,
            Meta.Signature signature, TimeZone timeZone, Meta.Frame firstFrame) {
        final ResultSetMetaData metaData =
                newResultSetMetaData(statement, signature);
        @SuppressWarnings("rawtypes")
        final CalcitePrepare.CalciteSignature calciteSignature =
        (CalcitePrepare.CalciteSignature) signature;
        return new CalciteResultSet(statement, calciteSignature, metaData, timeZone,
                firstFrame);
    }

    @Override
    public ResultSetMetaData newResultSetMetaData(AvaticaStatement statement,
            Meta.Signature signature) {
        return new AvaticaResultSetMetaData(statement, null, signature);
    }

    private static class PhoenixCalciteConnection extends CalciteConnectionImpl {
        public PhoenixCalciteConnection(Driver driver, AvaticaFactory factory, String url,
                Properties info, CalciteSchema rootSchema,
                JavaTypeFactory typeFactory) {
            super(driver, factory, url, info,
                    CalciteSchema.createRootSchema(true, false), typeFactory);
        }
        
        public void commit() throws SQLException {
            for (String subSchemaName : getRootSchema().getSubSchemaNames()) {               
                try {
                    PhoenixSchema phoenixSchema = getRootSchema()
                            .getSubSchema(subSchemaName).unwrap(PhoenixSchema.class);
                    phoenixSchema.pc.commit();
                } catch (ClassCastException e) {
                }
            }
        }
        
        public void close() throws SQLException {
            for (String subSchemaName : getRootSchema().getSubSchemaNames()) {               
                try {
                    PhoenixSchema phoenixSchema = getRootSchema()
                            .getSubSchema(subSchemaName).unwrap(PhoenixSchema.class);
                    phoenixSchema.pc.close();
                } catch (ClassCastException e) {
                }
            }
        }
    }

    private static class PhoenixCalciteStatement extends CalciteStatement {
        public PhoenixCalciteStatement(PhoenixCalciteConnection connection,
                Meta.StatementHandle h, int resultSetType, int resultSetConcurrency,
                int resultSetHoldability) {
            super(connection, h, resultSetType, resultSetConcurrency,
                    resultSetHoldability);
        }
    }

    private static class PhoenixCalcitePreparedStatement extends CalcitePreparedStatement {
        @SuppressWarnings("rawtypes")
        PhoenixCalcitePreparedStatement(PhoenixCalciteConnection connection,
                Meta.StatementHandle h, CalcitePrepare.CalciteSignature signature,
                int resultSetType, int resultSetConcurrency, int resultSetHoldability)
                        throws SQLException {
            super(connection, h, signature, resultSetType, resultSetConcurrency,
                    resultSetHoldability);
        }

        public void setRowId(
                int parameterIndex,
                RowId x) throws SQLException {
            getSite(parameterIndex).setRowId(x);
        }

        public void setNString(
                int parameterIndex, String value) throws SQLException {
            getSite(parameterIndex).setNString(value);
        }

        public void setNCharacterStream(
                int parameterIndex,
                Reader value,
                long length) throws SQLException {
            getSite(parameterIndex)
            .setNCharacterStream(value, length);
        }

        public void setNClob(
                int parameterIndex,
                NClob value) throws SQLException {
            getSite(parameterIndex).setNClob(value);
        }

        public void setClob(
                int parameterIndex,
                Reader reader,
                long length) throws SQLException {
            getSite(parameterIndex)
            .setClob(reader, length);
        }

        public void setBlob(
                int parameterIndex,
                InputStream inputStream,
                long length) throws SQLException {
            getSite(parameterIndex)
            .setBlob(inputStream, length);
        }

        public void setNClob(
                int parameterIndex,
                Reader reader,
                long length) throws SQLException {
            getSite(parameterIndex).setNClob(reader, length);
        }

        public void setSQLXML(
                int parameterIndex, SQLXML xmlObject) throws SQLException {
            getSite(parameterIndex).setSQLXML(xmlObject);
        }

        public void setAsciiStream(
                int parameterIndex,
                InputStream x,
                long length) throws SQLException {
            getSite(parameterIndex)
            .setAsciiStream(x, length);
        }

        public void setBinaryStream(
                int parameterIndex,
                InputStream x,
                long length) throws SQLException {
            getSite(parameterIndex)
            .setBinaryStream(x, length);
        }

        public void setCharacterStream(
                int parameterIndex,
                Reader reader,
                long length) throws SQLException {
            getSite(parameterIndex)
            .setCharacterStream(reader, length);
        }

        public void setAsciiStream(
                int parameterIndex, InputStream x) throws SQLException {
            getSite(parameterIndex).setAsciiStream(x);
        }

        public void setBinaryStream(
                int parameterIndex, InputStream x) throws SQLException {
            getSite(parameterIndex).setBinaryStream(x);
        }

        public void setCharacterStream(
                int parameterIndex, Reader reader) throws SQLException {
            getSite(parameterIndex)
            .setCharacterStream(reader);
        }

        public void setNCharacterStream(
                int parameterIndex, Reader value) throws SQLException {
            getSite(parameterIndex)
            .setNCharacterStream(value);
        }

        public void setClob(
                int parameterIndex,
                Reader reader) throws SQLException {
            getSite(parameterIndex).setClob(reader);
        }

        public void setBlob(
                int parameterIndex, InputStream inputStream) throws SQLException {
            getSite(parameterIndex)
            .setBlob(inputStream);
        }

        public void setNClob(
                int parameterIndex, Reader reader) throws SQLException {
            getSite(parameterIndex)
            .setNClob(reader);
        }
    }

    /** Implementation of database metadata for JDBC 4.1. */
    private static class PhoenixCalciteDatabaseMetaData
    extends AvaticaDatabaseMetaData {
        PhoenixCalciteDatabaseMetaData(PhoenixCalciteConnection connection) {
            super(connection);
        }
    }
}
