package org.apache.phoenix.calcite.metadata;

import java.util.List;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
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
import org.apache.phoenix.calcite.rel.PhoenixMergeSortUnion;
import org.apache.phoenix.calcite.rel.PhoenixServerJoin;
import org.apache.phoenix.calcite.rel.PhoenixTableScan;

import com.google.common.collect.ImmutableList;

public class PhoenixRelMdCollation implements MetadataHandler<BuiltInMetadata.Collation> {
    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                BuiltInMethod.COLLATIONS.method, new PhoenixRelMdCollation());

    private PhoenixRelMdCollation() { }

    @Override
    public MetadataDef<BuiltInMetadata.Collation> getDef() {
        return BuiltInMetadata.Collation.DEF;
    }

    public ImmutableList<RelCollation> collations(PhoenixTableScan tableScan, RelMetadataQuery mq) {
        return ImmutableList.copyOf(tableScan.getCollationList());
    }

    public ImmutableList<RelCollation> collations(PhoenixCorrelate correlate, RelMetadataQuery mq) {
        return ImmutableList.copyOf(correlate(mq, correlate.getLeft(), correlate.getRight(), correlate.getJoinType()));
    }

    public ImmutableList<RelCollation> collations(PhoenixLimit limit, RelMetadataQuery mq) {
        return ImmutableList.copyOf(RelMdCollation.limit(mq, limit.getInput()));
    }

    public ImmutableList<RelCollation> collations(PhoenixServerJoin join, RelMetadataQuery mq) {
        return ImmutableList.copyOf(hashJoin(mq, join.getLeft(), join.getRight(), join.getJoinType()));
    }

    public ImmutableList<RelCollation> collations(PhoenixClientJoin join, RelMetadataQuery mq) {
        return ImmutableList.copyOf(PhoenixRelMdCollation.mergeJoin(mq, join.getLeft(), join.getRight(), join.joinInfo.leftKeys, join.joinInfo.rightKeys));
    }

    public ImmutableList<RelCollation> collations(PhoenixMergeSortUnion union, RelMetadataQuery mq) {
        return ImmutableList.of(union.collation);
    }
    
    /** Helper method to determine a {@link PhoenixCorrelate}'s collation. */
    public static List<RelCollation> correlate(RelMetadataQuery mq, RelNode left, RelNode right, SemiJoinType joinType) {
        return mq.collations(left);
    }
    
    /** Helper method to determine a {@link PhoenixServerJoin}'s collation. */
    public static List<RelCollation> hashJoin(RelMetadataQuery mq, RelNode left, RelNode right, JoinRelType joinType) {
        return mq.collations(left);
    }

    public static List<RelCollation> mergeJoin(RelMetadataQuery mq, RelNode left, RelNode right,
            ImmutableIntList leftKeys, ImmutableIntList rightKeys) {
        final ImmutableList.Builder<RelCollation> builder = ImmutableList.builder();

        final ImmutableList<RelCollation> leftCollations =
                mq.collations(left);
        for (RelCollation collation : leftCollations) {
            if (!collation.getFieldCollations().isEmpty()) {
                builder.add(collation);
            }
        }
        
        final ImmutableList<RelCollation> rightCollations =
                mq.collations(right);
        final int leftFieldCount = left.getRowType().getFieldCount();
        for (RelCollation collation : rightCollations) {
            if (!collation.getFieldCollations().isEmpty()) {
                builder.add(RelCollations.shift(collation, leftFieldCount));
            }
        }
        return builder.build();
    }

}
