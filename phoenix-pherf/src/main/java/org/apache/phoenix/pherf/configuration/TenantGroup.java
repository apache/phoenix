package org.apache.phoenix.pherf.configuration;

import javax.xml.bind.annotation.XmlAttribute;

public class TenantGroup {
    private String id;
    private int weight;
    private int numTenants;

    @XmlAttribute
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @XmlAttribute
    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    @XmlAttribute
    public int getNumTenants() { return numTenants; }

    public void setNumTenants(int numTenants) { this.numTenants = numTenants; }


}
