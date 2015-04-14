package org.apache.phoenix.calcite.rel;

import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.phoenix.parse.JoinTableNode.JoinType;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Join}
 * relational expression in Phoenix.
 */
abstract public class PhoenixAbstractJoin extends Join implements PhoenixRel {
    public PhoenixAbstractJoin(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition, JoinRelType joinType, Set<String> variablesStopped) {
        super( cluster, traits, left, right, condition, joinType, variablesStopped);
        assert getConvention() == PhoenixRel.CONVENTION;
    }
    
    protected static JoinType convertJoinType(JoinRelType type) {
        JoinType ret = null;
        switch (type) {
        case INNER:
            ret = JoinType.Inner;
            break;
        case LEFT:
            ret = JoinType.Left;
            break;
        case RIGHT:
            ret = JoinType.Right;
            break;
        case FULL:
            ret = JoinType.Full;
            break;
        default:
        }
        
        return ret;
    }
}
