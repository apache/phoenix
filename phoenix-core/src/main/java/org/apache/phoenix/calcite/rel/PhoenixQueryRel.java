package org.apache.phoenix.calcite.rel;

import org.apache.phoenix.compile.QueryPlan;

public interface PhoenixQueryRel extends PhoenixRel {
    QueryPlan implement(PhoenixRelImplementor implementor);
}
