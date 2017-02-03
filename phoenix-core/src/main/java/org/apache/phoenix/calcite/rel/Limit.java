package org.apache.phoenix.calcite.rel;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

public abstract class Limit extends SingleRel {
    public final RexNode offset;
    public final RexNode fetch;

    protected Limit(RelOptCluster cluster, RelTraitSet traits, RelNode input, RexNode offset, RexNode fetch) {
        super(cluster, traits, input);
        this.offset = offset;
        this.fetch = fetch;
    }

    public abstract Limit copy(RelTraitSet traitSet, List<RelNode> newInputs);

    @Override 
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .itemIf("offset", offset, offset != null)
                .itemIf("fetch", fetch, fetch != null);
    }
    
    @Override 
    public double estimateRowCount(RelMetadataQuery mq) {
        double rows = super.estimateRowCount(mq);
        int offset = this.offset == null ? 0 : RexLiteral.intValue(this.offset);
        int fetch = this.fetch == null ? Integer.MAX_VALUE : RexLiteral.intValue(this.fetch);
        return Math.max(0, Math.min(fetch, rows - offset));
    }
}
