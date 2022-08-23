package org.apache.phoenix.jdbc.bootstrap;

public class EmbeddedDriverContextBuilder {
    private String quorum;
    private Integer port;
    private String rootNode;
    private String keytab;
    private String principal;

    public EmbeddedDriverContextBuilder setQuorum(String quorum) {
        this.quorum = quorum;
        return this;
    }

    public EmbeddedDriverContextBuilder setPort(Integer port) {
        this.port = port;
        return this;
    }

    public EmbeddedDriverContextBuilder setRootNode(String rootNode) {
        this.rootNode = rootNode;
        return this;
    }

    public EmbeddedDriverContextBuilder setPrincipal(String principal) {
        this.principal = principal;
        return this;
    }

    public EmbeddedDriverContextBuilder setKeytabFile(String keytab) {
        this.keytab = keytab;
        return this;
    }

    public EmbeddedDriverContext build() {
        return new EmbeddedDriverContext(quorum, port, rootNode, principal, keytab);
    }
}