package org.apache.phoenix.jdbc.bootstrap;

import org.apache.phoenix.jdbc.MalformedUrlException;
import org.apache.phoenix.thirdparty.com.google.common.base.Strings;
import org.apache.phoenix.util.PhoenixRuntime;

import java.sql.SQLException;

public class HBaseRegistryBootstrapFactory {

    public static HBaseRegistryBootstrap normalize(HBaseRegistryBootstrap from) {
        return resolve(from.getBootstrapType());
    }

    public static HBaseRegistryBootstrap resolve(HBaseRegistryBootstrapType theType) {
        if (theType == HBaseRegistryBootstrapType.HRPC) {
            return new HRpcHBaseRegistryBootstrap();
        }

        return new ZookeeperHBaseRegistryBootstrap();
    }

    public static ParsedHBaseRegistryBootstrapTuple fromURL(String url) throws SQLException {
        // ex: jdbc:phoenix+hrpc:hostname1....
        url = url.startsWith(PhoenixRuntime.JDBC_PROTOCOL)
                ? url.substring(PhoenixRuntime.JDBC_PROTOCOL.length())
                : PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + url;

        HBaseRegistryBootstrapType bootstrapAsEnum = null;

        // ex: +hrpc:hostname1....
        if (url.startsWith(String.valueOf(PhoenixRuntime.JDBC_PROTOCOL_CONNECTOR_PREFIX))) {
            int offset = url.indexOf(PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR);
            final String firstToken = url.substring(0, offset);

            // ex: +hrpc
            final String bootstrapString = firstToken
                    .replace(String.valueOf(PhoenixRuntime.JDBC_PROTOCOL_CONNECTOR_PREFIX), "")
                    .toUpperCase();

            if (Strings.isNullOrEmpty(bootstrapString)) {
                throw MalformedUrlException.getMalFormedUrlException(url);
            }

            // ex: hrpc
            try {
                bootstrapAsEnum = HBaseRegistryBootstrapType.valueOf(bootstrapString);
            } catch (IllegalArgumentException iae) {
                throw MalformedUrlException.getMalFormedUrlException("Invalid bootstrap connector specified: " + bootstrapString, url);
            }

            url = url.substring(firstToken.length());
        }

        return new ParsedHBaseRegistryBootstrapTuple(resolve(bootstrapAsEnum), url);
    }
}
