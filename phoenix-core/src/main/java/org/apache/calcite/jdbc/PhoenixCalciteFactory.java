package org.apache.calcite.jdbc;

import java.io.File;
import java.io.InputStream;
import java.io.Reader;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.ResultSet;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

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
import org.apache.calcite.avatica.remote.AvaticaHttpClientFactory;
import org.apache.calcite.avatica.remote.Service.Factory;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.Lex;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.UnregisteredDriver;
import org.apache.calcite.jdbc.CalciteConnectionImpl;
import org.apache.calcite.jdbc.CalciteFactory;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.model.JsonSchema.Type;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.phoenix.calcite.CalciteUtils;
import org.apache.phoenix.calcite.PhoenixSchema;
import org.apache.phoenix.calcite.PhoenixSqlConformance;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.RuntimeContext;
import org.apache.phoenix.jdbc.PhoenixConnection;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

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
                (Driver) driver, factory, url, info,
                CalciteSchema.createRootSchema(true, false), typeFactory);
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
        private final Map<Meta.StatementHandle, ImmutableList<RuntimeContext>> runtimeContextMap =
                new ConcurrentHashMap<Meta.StatementHandle, ImmutableList<RuntimeContext>>();
        
        public PhoenixCalciteConnection(Driver driver, AvaticaFactory factory, String url,
                Properties info, final CalciteSchema rootSchema,
                JavaTypeFactory typeFactory) {
            super(driver, factory, url, info, rootSchema, typeFactory);
        }

        @Override
        public CalciteConnectionConfig config() {
            final CalciteConnectionConfig config = super.config();
            return new DelegateCalciteConnectionConfig(config) {
                @Override
                public SqlConformance conformance() {
                    return PhoenixSqlConformance.INSTANCE;
                }
            };
        }

        @Override
        public CalciteStatement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            try {
                return super.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
            } catch (SQLException e) {
                throw CalciteUtils.unwrapSqlException(e);
            }
        }

        @Override
        public CalcitePreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            try {
                return super.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
            } catch (SQLException e) {
                throw CalciteUtils.unwrapSqlException(e);
            }
        }

        public <T> Enumerable<T> enumerable(Meta.StatementHandle handle,
                CalcitePrepare.CalciteSignature<T> signature) throws SQLException {
            Map<String, Object> map = Maps.newLinkedHashMap();
            AvaticaStatement statement = lookupStatement(handle);
            final List<TypedValue> parameterValues =
                    TROJAN.getParameterValues(statement);
            final Calendar calendar = Calendar.getInstance();
            for (Ord<TypedValue> o : Ord.zip(parameterValues)) {
                map.put("?" + o.i, o.e.toJdbc(calendar));
            }
            ImmutableList<RuntimeContext> ctxList = runtimeContextMap.get(handle);
            if (ctxList == null) {
                List<RuntimeContext> activeCtx = RuntimeContext.THREAD_LOCAL.get();
                ctxList = ImmutableList.copyOf(activeCtx);
                runtimeContextMap.put(handle, ctxList);
                activeCtx.clear();
            }
            for (RuntimeContext runtimeContext : ctxList) {
                runtimeContext.setBindParameterValues(map);
            }
            return super.enumerable(handle, signature);
        }

        @Override
        public void abort(final Executor executor) throws SQLException {
            call(new PhoenixConnectionCallable() {
                @Override
                public void call(PhoenixConnection conn) throws SQLException {
                    conn.abort(executor);
                }});
        }

        @Override
        public void rollback() throws SQLException {
            call(new PhoenixConnectionCallable() {
                @Override
                public void call(PhoenixConnection conn) throws SQLException {
                    conn.rollback();
                }});
        }

        @Override
        public void setReadOnly(final boolean readOnly) throws SQLException {
            call(new PhoenixConnectionCallable() {
                @Override
                public void call(PhoenixConnection conn) throws SQLException {
                    conn.setReadOnly(readOnly);
                }});
            super.setReadOnly(readOnly);
        }

        @Override
        public void setTransactionIsolation(final int level) throws SQLException {
            call(new PhoenixConnectionCallable() {
                @Override
                public void call(PhoenixConnection conn) throws SQLException {
                    conn.setTransactionIsolation(level);
                }});
            super.setTransactionIsolation(level);
        }

        @Override
        public void clearWarnings() throws SQLException {
            call(new PhoenixConnectionCallable() {
                @Override
                public void call(PhoenixConnection conn) throws SQLException {
                    conn.clearWarnings();
                }});
            super.clearWarnings();
        }

        @Override
        public Savepoint setSavepoint() throws SQLException {
            throw new SQLFeatureNotSupportedException();
        }

        @Override
        public Savepoint setSavepoint(String name) throws SQLException {
            throw new SQLFeatureNotSupportedException();
        }

        @Override
        public void rollback(final Savepoint savepoint) throws SQLException {
            throw new SQLFeatureNotSupportedException();
        }

        @Override
        public void releaseSavepoint(Savepoint savepoint) throws SQLException {
            throw new SQLFeatureNotSupportedException();
        }

        public void setAutoCommit(final boolean isAutoCommit) throws SQLException {
            call(new PhoenixConnectionCallable() {
                @Override
                public void call(PhoenixConnection conn) throws SQLException {
                    conn.setAutoCommit(isAutoCommit);;
                }});
        }
        
        public void commit() throws SQLException {
            call(new PhoenixConnectionCallable() {
                @Override
                public void call(PhoenixConnection conn) throws SQLException {
                    conn.commit();
                }});
        }
        
        public void close() throws SQLException {
            call(new PhoenixConnectionCallable() {
                @Override
                public void call(PhoenixConnection conn) throws SQLException {
                    conn.close();
                }});
            super.close();
        }
        
        private void call(PhoenixConnectionCallable callable) throws SQLException {
            for (String subSchemaName : getRootSchema().getSubSchemaNames()) {               
                try {
                    PhoenixSchema phoenixSchema = getRootSchema()
                            .getSubSchema(subSchemaName).unwrap(PhoenixSchema.class);
                    callable.call(phoenixSchema.pc);
                } catch (ClassCastException e) {
                }
            }
        }
        
        private static interface PhoenixConnectionCallable {
            void call(PhoenixConnection conn) throws SQLException;
        }

        @Override
        public DatabaseMetaData getMetaData() throws SQLException {
            for (String subSchemaName : getRootSchema().getSubSchemaNames()) {
                try {
                    PhoenixSchema phoenixSchema =
                            getRootSchema().getSubSchema(subSchemaName).unwrap(PhoenixSchema.class);
                    return phoenixSchema.pc.getMetaData();
                } catch (ClassCastException e) {
                }
            }
            return super.getMetaData();
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            if (iface.isInstance(this)) {
                return (T) this;
            }

            if (iface.isAssignableFrom(PhoenixConnection.class)) {
                SchemaPlus schema = getRootSchema().getSubSchema(this.getSchema());
                try {
                    return (T) (schema.unwrap(PhoenixSchema.class).pc);
                } catch (ClassCastException e) {
                }
            }

            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CLASS_NOT_UNWRAPPABLE)
                .setMessage(this.getClass().getName() + " not unwrappable from " + iface.getName())
                .build().buildException();
        }
    }

    private static class PhoenixCalciteStatement extends CalciteStatement {
        public PhoenixCalciteStatement(PhoenixCalciteConnection connection,
                Meta.StatementHandle h, int resultSetType, int resultSetConcurrency,
                int resultSetHoldability) {
            super(connection, h, resultSetType, resultSetConcurrency,
                    resultSetHoldability);
        }

        @Override
        public boolean execute(String sql) throws SQLException {
            try {
                return super.execute(sql);
            } catch (SQLException e) {
                throw CalciteUtils.unwrapSqlException(e);
            }
        }

        @Override
        public ResultSet executeQuery(String sql) throws SQLException{
            try {
                return super.executeQuery(sql);
            } catch (SQLException e) {
                throw CalciteUtils.unwrapSqlException(e);
            }
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

        @Override
        public boolean execute(String sql) throws SQLException {
            try {
                return super.execute(sql);
            } catch (SQLException e) {
                throw CalciteUtils.unwrapSqlException(e);
            }
        }

        @Override
        public ResultSet executeQuery(String sql) throws SQLException{
            try {
                return super.executeQuery(sql);
            } catch (SQLException e) {
                throw CalciteUtils.unwrapSqlException(e);
            }
        }

        public void setTimestamp(int parameterIndex, Timestamp x, Calendar calendar)
                throws SQLException {
            if (x != null) {
                x = new Timestamp(getAdjustedTime(x.getTime(), calendar));
            }
            super.setTimestamp(parameterIndex, x, calendar);
        }

        public void setDate(int parameterIndex, Date x, Calendar calendar)
                throws SQLException {
            if (x != null) {
                x = new Date(getAdjustedTime(x.getTime(), calendar));
            }
            super.setDate(parameterIndex, x, calendar);
        }

        public void setTime(int parameterIndex, Time x, Calendar calendar)
                throws SQLException {
            if (x != null) {
                x = new Time(getAdjustedTime(x.getTime(), calendar));
            }
            super.setTime(parameterIndex, x, calendar);
        }

        private long getAdjustedTime(long v, Calendar calendar) {
            return (v - calendar.getTimeZone().getOffset(v));
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

    private static class DelegateCalciteConnectionConfig implements CalciteConnectionConfig {
        private final CalciteConnectionConfig delegate;

        DelegateCalciteConnectionConfig(CalciteConnectionConfig delegate) {
            this.delegate = delegate;
        }

        @Override
        public String authentication() {
            return delegate.authentication();
        }

        @Override
        public String avaticaPassword() {
            return delegate.avaticaPassword();
        }

        @Override
        public String avaticaUser() {
            return delegate.avaticaUser();
        }

        @Override
        public Factory factory() {
            return delegate.factory();
        }

        @Override
        public String httpClientClass() {
            return delegate.httpClientClass();
        }

        @Override
        public AvaticaHttpClientFactory httpClientFactory() {
            return delegate.httpClientFactory();
        }

        @Override
        public File kerberosKeytab() {
            return delegate.kerberosKeytab();
        }

        @Override
        public String kerberosPrincipal() {
            return delegate.kerberosPrincipal();
        }

        @Override
        public String schema() {
            return delegate.schema();
        }

        @Override
        public String serialization() {
            return delegate.serialization();
        }

        @Override
        public String timeZone() {
            return delegate.timeZone();
        }

        @Override
        public String url() {
            return delegate.url();
        }

        @Override
        public boolean autoTemp() {
            return delegate.autoTemp();
        }

        @Override
        public boolean materializationsEnabled() {
            return delegate.materializationsEnabled();
        }

        @Override
        public boolean createMaterializations() {
            return delegate.createMaterializations();
        }

        @Override
        public NullCollation defaultNullCollation() {
            return delegate.defaultNullCollation();
        }

        @Override
        public <T> T fun(Class<T> operatorTableClass, T defaultOperatorTable) {
            return delegate.fun(operatorTableClass, defaultOperatorTable);
        }

        @Override
        public String model() {
            return delegate.model();
        }

        @Override
        public Lex lex() {
            return delegate.lex();
        }

        @Override
        public Quoting quoting() {
            return delegate.quoting();
        }

        @Override
        public Casing unquotedCasing() {
            return delegate.unquotedCasing();
        }

        @Override
        public Casing quotedCasing() {
            return delegate.quotedCasing();
        }

        @Override
        public boolean caseSensitive() {
            return delegate.caseSensitive();
        }

        @Override
        public <T> T parserFactory(Class<T> parserFactoryClass, T defaultParserFactory) {
            return delegate.parserFactory(parserFactoryClass, defaultParserFactory);
        }

        @Override
        public <T> T schemaFactory(Class<T> schemaFactoryClass, T defaultSchemaFactory) {
            return delegate.schemaFactory(schemaFactoryClass, defaultSchemaFactory);
        }

        @Override
        public Type schemaType() {
            return delegate.schemaType();
        }

        @Override
        public boolean spark() {
            return delegate.spark();
        }

        @Override
        public boolean forceDecorrelate() {
            return delegate.forceDecorrelate();
        }

        @Override
        public <T> T typeSystem(Class<T> typeSystemClass, T defaultTypeSystem) {
            return delegate.typeSystem(typeSystemClass, defaultTypeSystem);
        }

        @Override
        public SqlConformance conformance() {
            return delegate.conformance();
        }

        @Override
        public boolean approximateDistinctCount() {
            return delegate.approximateDistinctCount();
        }

        @Override
        public boolean approximateTopN() {
            return delegate.approximateTopN();
        }

        @Override
        public boolean approximateDecimal() {
            return delegate.approximateDecimal();
        }
    }
}
