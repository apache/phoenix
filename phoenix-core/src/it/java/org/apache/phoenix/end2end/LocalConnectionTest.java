package org.apache.phoenix.end2end;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import org.apache.phoenix.trace.TraceUtil;

import java.sql.*;

public class LocalConnectionTest {

    public static void dropTable(Connection connection, String tableName) throws SQLException {
        Span span = TraceUtil
            .getGlobalTracer().spanBuilder(String.format("delete-table-%s", tableName)).startSpan();
        try (Scope scope = span.makeCurrent()){
            Statement statement = connection.createStatement();
            statement.execute(String.format("DROP TABLE IF EXISTS %s", tableName));
            connection.commit();
            statement.close();
        } finally {
            span.end();
        }

    }

    public static void main(String[] args) throws SQLException {
        Statement stmt = null;
        ResultSet rset = null;
        String tableName = "phoenix_trace_test";
//        Span span = TraceUtil.getGlobalTracer().spanBuilder("phoenix-client-queries").startSpan();
//        try(Scope scope = span.makeCurrent()){
            Connection con = DriverManager.getConnection("jdbc:phoenix:localhost");
            dropTable(con, tableName);
            createTable(con, tableName);
            upsertIntoTable(con, tableName);
            scanTable(con, tableName);
            con.close();
//        } catch (Exception e){
//            System.out.println("Error in tracing: " + e);
//        } finally {
//            span.end();
//        }
    }

    private static void createTable(Connection connection, String tableName) throws SQLException {
        Span span = TraceUtil.getGlobalTracer().spanBuilder(String.format("create-table-%s", tableName)).startSpan();
        try (Scope scope = span.makeCurrent()){
            Statement statement = connection.createStatement();
            statement.execute(String.format("create table %s (mykey integer not null primary key, mycolumn varchar)", tableName));
            connection.commit();
            statement.close();
        } finally {
            span.end();
        }
    }

    private static void upsertIntoTable(Connection connection, String tableName) throws SQLException {
        Span span = TraceUtil.getGlobalTracer().spanBuilder(String.format("upsert-table-%s", tableName)).startSpan();
        try (Scope scope = span.makeCurrent()){
            Statement statement = connection.createStatement();
            for(int i = 0; i < 10000; i++){
                statement.executeUpdate(String.format("upsert into %s values (%s,'Hello-%s')", tableName, i, i));
            }
            connection.commit();
            statement.close();
        } finally {
            span.end();
        }
    }

    private static void scanTable(Connection connection, String tableName) throws SQLException {
        Span span = TraceUtil.getGlobalTracer().spanBuilder(String.format("scan-table-%s", tableName)).startSpan();
        try (Scope scope = span.makeCurrent()){
            Statement statement = connection.createStatement();
            statement.execute(String.format("select * from %s", tableName));
            statement.close();
        } finally {
            span.end();
        }
    }


}
