package org.apache.phoenix.pherf.configuration;

import javax.xml.bind.annotation.XmlAttribute;

public class OperationGroup {
    private String id;
    private int weight;

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
}
