package org.apache.phoenix.calcite.rel;

import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.phoenix.parse.JoinTableNode.JoinType;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Join}
 * relational expression in Phoenix.
 */
abstract public class PhoenixAbstractJoin extends Join implements PhoenixRel {
    public final JoinInfo joinInfo;
    public final boolean isSingleValueRhs;

    protected PhoenixAbstractJoin(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition, JoinRelType joinType, Set<String> variablesStopped, boolean isSingleValueRhs) {
        super( cluster, traits, left, right, condition, joinType, variablesStopped);
        this.joinInfo = JoinInfo.of(left, right, condition);
        this.isSingleValueRhs = isSingleValueRhs;
        assert getConvention() == PhoenixRel.CONVENTION;
    }
    
    abstract public PhoenixAbstractJoin copy(RelTraitSet traits, RexNode condition, RelNode left,
            RelNode right, JoinRelType joinRelType, boolean semiJoinDone, boolean isSingleValueRhs);

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .itemIf("isSingleValueRhs", isSingleValueRhs, isSingleValueRhs);
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
