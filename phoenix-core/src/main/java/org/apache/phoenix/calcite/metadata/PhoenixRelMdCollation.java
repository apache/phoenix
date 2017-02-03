package org.apache.phoenix.calcite.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SemiJoinType;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.phoenix.calcite.rel.Limit;
import org.apache.phoenix.calcite.rel.PhoenixClientJoin;
import org.apache.phoenix.calcite.rel.PhoenixCorrelate;
import org.apache.phoenix.calcite.rel.PhoenixMergeSortUnion;
import org.apache.phoenix.calcite.rel.PhoenixServerJoin;
import org.apache.phoenix.calcite.rel.PhoenixTableScan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

public class PhoenixRelMdCollation implements MetadataHandler<BuiltInMetadata.Collation> {
    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                BuiltInMethod.COLLATIONS.method, new PhoenixRelMdCollation());

    private PhoenixRelMdCollation() { }

    @Override
    public MetadataDef<BuiltInMetadata.Collation> getDef() {
        return BuiltInMetadata.Collation.DEF;
    }

    public ImmutableList<RelCollation> collations(Project project, RelMetadataQuery mq) {
        return ImmutableList.copyOf(project(mq, project.getInput(), project.getProjects()));
    }

    public ImmutableList<RelCollation> collations(PhoenixTableScan tableScan, RelMetadataQuery mq) {
        return ImmutableList.copyOf(tableScan.getCollationList());
    }

    public ImmutableList<RelCollation> collations(PhoenixCorrelate correlate, RelMetadataQuery mq) {
        return ImmutableList.copyOf(correlate(mq, correlate.getLeft(), correlate.getRight(), correlate.getJoinType()));
    }

    public ImmutableList<RelCollation> collations(Limit limit, RelMetadataQuery mq) {
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

    public static List<RelCollation> project(RelMetadataQuery mq,
            RelNode input, List<? extends RexNode> projects) {
        final SortedSet<RelCollation> collations = new TreeSet<>();
        final List<RelCollation> inputCollations = mq.collations(input);
        if (inputCollations == null || inputCollations.isEmpty()) {
            return ImmutableList.of();
        }
        final Multimap<Integer, Integer> targets = LinkedListMultimap.create();
        final Map<Integer, SqlMonotonicity> targetsWithMonotonicity =
                new HashMap<>();
        for (Ord<? extends RexNode> project : Ord.zip(projects)) {
            if (project.e instanceof RexInputRef) {
                targets.put(((RexInputRef) project.e).getIndex(), project.i);
            } else if (project.e instanceof RexCall) {
                final RexCall call = (RexCall) project.e;
                final RexCallBinding binding =
                        RexCallBinding.create(input.getCluster().getTypeFactory(), call, inputCollations);
                targetsWithMonotonicity.put(project.i, call.getOperator().getMonotonicity(binding));
            }
        }
        final List<RelFieldCollation> fieldCollations = new ArrayList<>();
            for (RelCollation ic : inputCollations) {
                if (ic.getFieldCollations().isEmpty()) {
                    continue;
                }
                fieldCollations.clear();
                for (RelFieldCollation ifc : ic.getFieldCollations()) {
                    final Collection<Integer> integers = targets.get(ifc.getFieldIndex());
                    if (integers.isEmpty()) {
                        break;
                    }
                    fieldCollations.add(ifc.copy(integers.iterator().next()));
                }
                if (!fieldCollations.isEmpty()) {
                    collations.add(RelCollations.of(fieldCollations));
                }
            }

        final List<RelFieldCollation> fieldCollationsForRexCalls =
                new ArrayList<>();
        for (Map.Entry<Integer, SqlMonotonicity> entry
                : targetsWithMonotonicity.entrySet()) {
            final SqlMonotonicity value = entry.getValue();
            switch (value) {
            case NOT_MONOTONIC:
            case CONSTANT:
                break;
            default:
                fieldCollationsForRexCalls.add(
                        new RelFieldCollation(entry.getKey(),
                                RelFieldCollation.Direction.of(value)));
                break;
            }
        }

        if (!fieldCollationsForRexCalls.isEmpty()) {
            collations.add(RelCollations.of(fieldCollationsForRexCalls));
        }

        return ImmutableList.copyOf(collations);
    }
}
