package org.apache.phoenix.calcite.metadata;

import java.util.List;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SemiJoinType;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.phoenix.calcite.rel.PhoenixClientJoin;
import org.apache.phoenix.calcite.rel.PhoenixCorrelate;
import org.apache.phoenix.calcite.rel.PhoenixLimit;
import org.apache.phoenix.calcite.rel.PhoenixServerJoin;

import com.google.common.collect.ImmutableList;

public class PhoenixRelMdCollation {
    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                BuiltInMethod.COLLATIONS.method, new PhoenixRelMdCollation());

    private PhoenixRelMdCollation() { }

    public ImmutableList<RelCollation> collations(PhoenixCorrelate correlate) {
        return ImmutableList.copyOf(correlate(correlate.getLeft(), correlate.getRight(), correlate.getJoinType()));
    }

    public ImmutableList<RelCollation> collations(PhoenixLimit limit) {
        return ImmutableList.copyOf(RelMdCollation.limit(limit.getInput()));
    }

    public ImmutableList<RelCollation> collations(PhoenixServerJoin join) {
        return ImmutableList.copyOf(hashJoin(join.getLeft(), join.getRight(), join.getJoinType()));
    }

    public ImmutableList<RelCollation> collations(PhoenixClientJoin join) {
        return ImmutableList.copyOf(PhoenixRelMdCollation.mergeJoin(join.getLeft(), join.getRight(), join.joinInfo.leftKeys, join.joinInfo.rightKeys));
    }
    
    /** Helper method to determine a {@link PhoenixCorrelate}'s collation. */
    public static List<RelCollation> correlate(RelNode left, RelNode right, SemiJoinType joinType) {
        return RelMetadataQuery.collations(left);
    }
    
    /** Helper method to determine a {@link PhoenixServerJoin}'s collation. */
    public static List<RelCollation> hashJoin(RelNode left, RelNode right, JoinRelType joinType) {
        return RelMetadataQuery.collations(left);
    }

    public static List<RelCollation> mergeJoin(RelNode left, RelNode right,
            ImmutableIntList leftKeys, ImmutableIntList rightKeys) {
        final ImmutableList.Builder<RelCollation> builder = ImmutableList.builder();

        final ImmutableList<RelCollation> leftCollations =
                RelMetadataQuery.collations(left);
        for (RelCollation collation : leftCollations) {
            if (!collation.getFieldCollations().isEmpty()) {
                builder.add(collation);
            }
        }
        
        final ImmutableList<RelCollation> rightCollations =
                RelMetadataQuery.collations(right);
        final int leftFieldCount = left.getRowType().getFieldCount();
        for (RelCollation collation : rightCollations) {
            if (!collation.getFieldCollations().isEmpty()) {
                builder.add(RelCollations.shift(collation, leftFieldCount));
            }
        }
        return builder.build();
    }

}
