package org.apache.phoenix.parse;

import org.apache.phoenix.util.SchemaUtil;

public class CursorName {
    private final String name;
    private final boolean isCaseSensitive;

    public CursorName(String name, boolean isCaseSensitive){
        this.name = name;
        this.isCaseSensitive = isCaseSensitive;
    }

    public CursorName(String name){
        this.name = name;
        this.isCaseSensitive = name == null ? false: SchemaUtil.isCaseSensitive(name);
    }

    public String getName() {
        return name;
    }

    public boolean isCaseSensitive() {
        return isCaseSensitive;
    }
}
