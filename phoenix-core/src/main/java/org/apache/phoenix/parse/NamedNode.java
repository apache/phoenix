package org.apache.phoenix.parse;

import org.apache.phoenix.util.SchemaUtil;

public class NamedNode {
    private final String name;
    private final boolean isCaseSensitive;
    
    public static NamedNode caseSensitiveNamedNode(String name) {
        return new NamedNode(name,true);
    }
    
    private NamedNode(String name, boolean isCaseSensitive) {
        this.name = name;
        this.isCaseSensitive = isCaseSensitive;
    }

    NamedNode(String name) {
        this.name = SchemaUtil.normalizeIdentifier(name);
        this.isCaseSensitive = name == null ? false : SchemaUtil.isCaseSensitive(name);
    }

    public String getName() {
        return name;
    }

    public boolean isCaseSensitive() {
        return isCaseSensitive;
    }
    
    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        NamedNode other = (NamedNode)obj;
        return name.equals(other.name);
    }

}
