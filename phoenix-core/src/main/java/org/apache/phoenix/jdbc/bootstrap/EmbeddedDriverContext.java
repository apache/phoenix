package org.apache.phoenix.jdbc.bootstrap;

public class EmbeddedDriverContext {

    private final String quorum;
    private final Integer port;
    private final String rootNode;
    private final String principal;
    private final String keytabFile;


    protected EmbeddedDriverContext(String quorum, Integer port, String rootNode, String principal, String keytabFile) {
        this.quorum = quorum;
        this.port = port;
        this.rootNode = rootNode;
        this.principal = principal;
        this.keytabFile = keytabFile;
    }

    public String getQuorum() {
        return quorum;
    }

    public Integer getPort() {
        return port;
    }

    public String getRootNode() {
        return rootNode;
    }

    public String getPrincipal() {
        return principal;
    }

    public String getKeytabFile() {
        return keytabFile;
    }
}
