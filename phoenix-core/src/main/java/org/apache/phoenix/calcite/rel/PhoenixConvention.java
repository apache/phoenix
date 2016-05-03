package org.apache.phoenix.calcite.rel;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;

public enum PhoenixConvention implements Convention {
    
    /** Generic convention*/
    GENERIC,
    
    /** Server convention*/
    SERVER,
    
    /** Server join convention*/
    SERVERJOIN,
    
    /** Client convention*/
    CLIENT,
    
    /** Mutation convention*/
    MUTATION;

    @Override
    public RelTraitDef<?> getTraitDef() {
        return ConventionTraitDef.INSTANCE;
    }

    @Override
    public boolean satisfies(RelTrait trait) {
        return this == trait || (this != MUTATION && trait == GENERIC);
    }

    @Override
    public void register(RelOptPlanner planner) {
    }

    @Override
    public Class<PhoenixRel> getInterface() {
        return PhoenixRel.class;
    }

    @Override
    public String getName() {
        return "PHOENIX_" + this.name();
    }

}
