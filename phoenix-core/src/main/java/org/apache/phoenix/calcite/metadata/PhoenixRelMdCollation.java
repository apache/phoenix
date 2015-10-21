package org.apache.phoenix.calcite.metadata;

import java.util.List;
import java.util.Set;

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
import org.apache.phoenix.calcite.rel.PhoenixMergeSortUnion;
import org.apache.phoenix.calcite.rel.PhoenixServerJoin;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

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

    public ImmutableList<RelCollation> collations(PhoenixMergeSortUnion union) {
        return ImmutableList.copyOf(PhoenixRelMdCollation.mergeSortUnion(union.getInputs(), union.all));
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
    
    public static List<RelCollation> mergeSortUnion(List<RelNode> inputs, boolean all) {
    	if (!all) {
    		return ImmutableList.of(RelCollations.EMPTY);
    	}
    	
    	Set<RelCollation> mergedCollations = null;
    	for (RelNode input : inputs) {
    		final ImmutableList<RelCollation> inputCollations = RelMetadataQuery.collations(input);
    		Set<RelCollation> nonEmptyInputCollations = Sets.newHashSet();
			for (RelCollation collation : inputCollations) {
				if (!collation.getFieldCollations().isEmpty()) {
					nonEmptyInputCollations.add(collation);
				}
			}
    		
			if (nonEmptyInputCollations.isEmpty() || mergedCollations == null) {
    			mergedCollations = nonEmptyInputCollations;
    		} else {
    			Set<RelCollation> newCollations = Sets.newHashSet();
    			for (RelCollation m : mergedCollations) {
    				for (RelCollation n : nonEmptyInputCollations) {
    					if (n.satisfies(m)) {
    						newCollations.add(m);
    						break;
    					}
    				}
    			}
    			for (RelCollation n : nonEmptyInputCollations) {
    				for (RelCollation m : mergedCollations) {
    					if (m.satisfies(n)) {
    						newCollations.add(n);
    						break;
    					}
    				}
    			}
    			mergedCollations = newCollations;
    		}
			
    		if (mergedCollations.isEmpty()) {
    			break;
    		}
    	}
    	
    	// We only return the simplified collation here because PhoenixMergeSortUnion
    	// needs a definite way for implement().
		if (mergedCollations.size() != 1) {
			return ImmutableList.of(RelCollations.EMPTY);
		}
        return ImmutableList.of(mergedCollations.iterator().next());
    }

}
