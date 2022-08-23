package org.apache.phoenix.jdbc.bootstrap;

import org.apache.phoenix.jdbc.MalformedUrlException;
import org.apache.phoenix.thirdparty.com.google.common.base.Strings;
import org.apache.phoenix.util.PhoenixRuntime;

import java.sql.SQLException;
import java.util.StringTokenizer;

public class HBaseRegistryBootstrapFactory {

    private static final String TERMINATOR = "" + PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
    private static final String DELIMITERS = TERMINATOR + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;

    private static final char WINDOWS_SEPARATOR_CHAR = '\\';

    public static HBaseRegistryBootstrap fromURL(String url) throws SQLException {
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

            // ex: HRPC
            try {
                bootstrapAsEnum = HBaseRegistryBootstrapType.valueOf(bootstrapString);
            } catch (IllegalArgumentException iae) {
                throw MalformedUrlException.getMalFormedUrlException("Invalid bootstrap connector specified: " + bootstrapString, url);
            }

            url = url.substring(firstToken.length());
        }


        StringTokenizer tokenizer = new StringTokenizer(url, DELIMITERS, true);
        int nTokens = 0;
        String[] tokens = new String[5];
        String token = null;
        while (tokenizer.hasMoreTokens() &&
                !(token = tokenizer.nextToken()).equals(TERMINATOR) &&
                tokenizer.hasMoreTokens() && nTokens < tokens.length) {
            token = tokenizer.nextToken();
            // This would mean we have an empty string for a token which is illegal
            if (DELIMITERS.contains(token)) {
                throw MalformedUrlException.getMalFormedUrlException(url);
            }
            tokens[nTokens++] = token;
        }
        // Look-forward to see if the last token is actually the C:\\ path
        if (tokenizer.hasMoreTokens() && !TERMINATOR.equals(token)) {
            String extraToken = tokenizer.nextToken();
            if (WINDOWS_SEPARATOR_CHAR == extraToken.charAt(0)) {
                String prevToken = tokens[nTokens - 1];
                tokens[nTokens - 1] = prevToken + ":" + extraToken;
                if (tokenizer.hasMoreTokens() && !(token = tokenizer.nextToken()).equals(TERMINATOR)) {
                    throw MalformedUrlException.getMalFormedUrlException(url);
                }
            } else {
                throw MalformedUrlException.getMalFormedUrlException(url);
            }
        }
        String quorum = null;
        Integer port = null;
        String rootNode = null;
        String principal = null;
        String keytabFile = null;
        int tokenIndex = 0;
        if (nTokens > tokenIndex) {
            quorum = tokens[tokenIndex++]; // Found quorum
            if (nTokens > tokenIndex) {
                try {
                    port = Integer.parseInt(tokens[tokenIndex]);
                    if (port < 0) {
                        throw MalformedUrlException.getMalFormedUrlException(url);
                    }
                    tokenIndex++; // Found port
                } catch (NumberFormatException e) { // No port information
                    if (isMultiPortUrl(tokens[tokenIndex])) {
                        throw MalformedUrlException.getMalFormedUrlException(url);
                    }
                }
                if (nTokens > tokenIndex) {
                    if (tokens[tokenIndex].startsWith("/")) {
                        rootNode = tokens[tokenIndex++]; // Found rootNode
                    }
                    if (nTokens > tokenIndex) {
                        principal = tokens[tokenIndex++]; // Found principal
                        if (nTokens > tokenIndex) {
                            keytabFile = tokens[tokenIndex++]; // Found keytabFile
                            // There's still more after, try to see if it's a windows file path
                            if (tokenIndex < tokens.length) {
                                String nextToken = tokens[tokenIndex++];
                                // The next token starts with the directory separator, assume
                                // it's still the keytab path.
                                if (null != nextToken && WINDOWS_SEPARATOR_CHAR == nextToken.charAt(0)) {
                                    keytabFile = keytabFile + ":" + nextToken;
                                }
                            }
                        }
                    }
                }
            }
        }

        // 🥴
        final EmbeddedDriverContext edc = new EmbeddedDriverContextBuilder()
                .setQuorum(quorum).setPort(port)
                .setRootNode(rootNode).setPrincipal(principal).setKeytabFile(keytabFile)
                .build();

        if (bootstrapAsEnum == HBaseRegistryBootstrapType.HRPC) {
            return new HRpcHBaseRegistryBootstrap(edc);
        }

        if (edc.getQuorum() != null) {
            return new ZookeeperHBaseRegistryBootstrap(edc);
        }

        return null;
    }

    /**
     * Detect url with quorum:1,quorum:2 as HBase does not handle different port numbers
     * for different quorum hostnames.
     *
     * @param portStr
     * @return
     */
    private static boolean isMultiPortUrl(String portStr) {
        int commaIndex = portStr.indexOf(',');
        if (commaIndex > 0) {
            try {
                Integer.parseInt(portStr.substring(0, commaIndex));
                return true;
            } catch (NumberFormatException otherE) {
            }
        }
        return false;
    }

}
