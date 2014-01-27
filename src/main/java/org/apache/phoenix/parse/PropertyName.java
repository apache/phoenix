package org.apache.phoenix.parse;

import org.apache.phoenix.util.SchemaUtil;

public class PropertyName {
    private final NamedNode familyName;
    private final String propertyName;
    
    PropertyName(String familyName, String propertyName) {
        this.familyName = familyName == null ? null : new NamedNode(familyName);
        this.propertyName = SchemaUtil.normalizeIdentifier(propertyName);;
    }

    PropertyName(String columnName) {
        this(null, columnName);
    }

    public String getFamilyName() {
        return familyName == null ? "" : familyName.getName();
    }

    public String getPropertyName() {
        return propertyName;
    }
}