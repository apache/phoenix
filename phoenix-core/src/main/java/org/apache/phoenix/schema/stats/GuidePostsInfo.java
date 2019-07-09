/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.schema.stats;

import java.util.List;
import java.util.LinkedList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ScanUtil.BytesComparator;
import org.apache.phoenix.query.KeyRange;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@VisibleForTesting
final class DecodedGuidePostChunk {
    /**
     * The index of the guide post chunk in the chunk array.
     */
    private final int guidePostChunkIndex;

    /**
     * The key range of the guide posts in this chunk
     */
    private KeyRange keyRange;

    /**
     * The guide posts in this chunk.
     */
    private final List<byte[]> guidePosts;

    public DecodedGuidePostChunk(int guidePostChunkIndex, KeyRange keyRange, List<byte[]> guidePosts) {
        Preconditions.checkArgument(guidePostChunkIndex != GuidePostChunk.INVALID_GUIDEPOST_CHUNK_INDEX);
        Preconditions.checkArgument(guidePosts.size() > 0);

        this.guidePostChunkIndex = guidePostChunkIndex;
        this.keyRange = keyRange;
        this.guidePosts = guidePosts;
    }

    public int getGuidePostChunkIndex() {
        return guidePostChunkIndex;
    }

    public List<byte[]> getGuidePosts() {
        return guidePosts;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DecodedGuidePostChunk)) {
            return false;
        }

        DecodedGuidePostChunk that = (DecodedGuidePostChunk)o;
        if (this.guidePostChunkIndex != that.guidePostChunkIndex
                || this.keyRange != that.keyRange
                || this.guidePosts.size() != that.guidePosts.size()) {
            return false;
        }

        for (int i = 0; i < this.guidePosts.size(); i++) {
            if (Bytes.BYTES_COMPARATOR.compare(this.guidePosts.get(i), that.guidePosts.get(i)) != 0) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        return this.guidePostChunkIndex;
    }

    /**
     * The guide post boundaries:
     *     gp_0, gp_1, ..., gp_i0, ..., gp_i1, ..., gp_i2, ..., gp_in, ..., gp_n
     * The guide post boundaries:
     *     gp_i0, gp_i1, ..., gp_in, ..., gp_n
     * The key space split by the guide post chunks:
     *     (UNBOUND, gp_i0](gp_i0, gp_i1](gp_i1, gp_i2]...(gp_in, gp_n](gp_n, UNBOUND)
     * The last guide post chunk is an ending chunk which contains one ending guide post
     * KeyRang.UNBOUND with 0 estimated rows/bytes and Long.MAX last update timestamp.
     * @param key
     * @param isLowerBound
     * @return the zero-based local index of the guide post with the given key
     */
    public int locateGuidePost(byte[] key, boolean isLowerBound) {
        if (guidePosts.get(0) == KeyRange.UNBOUND) {
            return 0;
        }

        if (key == keyRange.UNBOUND) {
            return isLowerBound ? 0 : guidePosts.size();
        }

        // case #    isLowerBound    Found exact match (index >= 0)?    index returned
        //   1           Y                      Y                         (ret + 1)
        //     comment for case 1 above:
        //         If lower bound is inclusive, return the ret + 1, as the current guide post only
        //              contains one row, so ignore it.
        //         If lower bound is exclusive, return the ret + 1, as the exclusive key will be
        //              contained in the next guide post and we're matching on the end key of the guide post.
        //   2           Y                      N                         -(ret + 1)
        //   3           N                      Y                            ret
        //   4           N                      N                         -(ret + 1)
        int ret = Collections.binarySearch(guidePosts, key, Bytes.BYTES_COMPARATOR);
        if (ret < 0) {
            ret = -(ret + 1);
        } else if (isLowerBound) {
            ret += 1;
        }

        return ret;
    }
}

@VisibleForTesting
class DecodedGuidePostChunkCache {
    /**
     * The guide post chunks which contains encoded guide posts.
     */
    private final List<GuidePostChunk> guidePostChunks;

    /**
     * The cached guide post chunks which contains decode guide posts.
     */
    private final List<DecodedGuidePostChunk> cachedDecodedGuidePostChunks;

    public DecodedGuidePostChunkCache(List<GuidePostChunk> guidePostChunks) {
        this.guidePostChunks = guidePostChunks;
        cachedDecodedGuidePostChunks = new LinkedList<>();
    }

    public List<DecodedGuidePostChunk> getCachedDecodedGuidePostChunks() {
        return cachedDecodedGuidePostChunks;
    }

    /**
     * This is a read-through cache to hold the decoded guide post chunks which were accessed recently.
     * Because this is a session based cache, which means the cache instance only exists during a query,
     * there is no need for providing expiration policy for cache entries.
     *
     * During a query, the access pattern is always to find the start chunk and end chunk, then sequentially
     * access all the chunks between start chunk and end chunk inclusively. To minimize the memory footprint,
     * when this function is called with removePreviousEntries being set to 'true', all the chunks with chunk
     * index less than the chunk index to search are removed from the cache.
     *
     * @param chunkIndex
     *          The index of the chunk to search
     * @param removePreviousEntries
     *          When it is true,  all the chunks with chunk index less than the chunk index to search
     *          are removed from the cache.
     * @return
     */
    public DecodedGuidePostChunk get(int chunkIndex, boolean removePreviousEntries) {
        if (chunkIndex < 0 || chunkIndex >= guidePostChunks.size()) {
            return null;
        }

        int listIndex = 0;
        while (listIndex < cachedDecodedGuidePostChunks.size()) {
            int currentChunkIndex = cachedDecodedGuidePostChunks.get(listIndex).getGuidePostChunkIndex();
            if (currentChunkIndex < chunkIndex) {
                if (removePreviousEntries) {
                    cachedDecodedGuidePostChunks.remove(listIndex);
                } else {
                    listIndex++;
                }
            } else if (currentChunkIndex == chunkIndex) {
                return cachedDecodedGuidePostChunks.get(listIndex);
            } else {
                break;
            }
        }

        DecodedGuidePostChunk decodedChunk = guidePostChunks.get(chunkIndex).decode();
        cachedDecodedGuidePostChunks.add(listIndex, decodedChunk);
        return decodedChunk;
    }
}

@VisibleForTesting
class GuidePostIterator implements Iterator<GuidePost> {
    /**
     * The guide post chunks. The guide posts held by this tree are split into chunks
     */
    private final List<GuidePostChunk> guidePostChunks;

    /**
     * The index of the first guide post in the search range, which is the local index
     * (Zero-based) in the first returned chunk.
     */
    private final int indexOfFirstGuidePost;

    /**
     * The index of the last guide post in the search range, which is the local index
     * (Zero-based) in the last returned chunk.
     */
    private final int indexOfLastGuidePost;

    /**
     * The index of the first returned chunks
     */
    private final int indexOfFirstChunk;

    /**
     * The index of the last returned chunks
     */
    private final int indexOfLastChunk;

    /**
     * The cache for the decoded guide post chunks
     */
    private final DecodedGuidePostChunkCache decodedGuidePostChunkCache;

    private int indexOfCurrentGuidePost;

    private int indexOfCurrentGuidePostChunk;

    private DecodedGuidePostChunk decodedGuidePostChunk;

    /**
     *
     * @param guidePostChunks
     * @param decodedGuidePostChunkCache
     * @param indexOfFirstChunk
     * @param indexOfFirstGuidePost
     * @param indexOfLastChunk - inclusive
     * @param indexOfLastGuidePost - inclusive
     */
    public GuidePostIterator(List<GuidePostChunk> guidePostChunks,
            DecodedGuidePostChunkCache decodedGuidePostChunkCache,
            int indexOfFirstChunk, int indexOfFirstGuidePost,
            int indexOfLastChunk, int indexOfLastGuidePost) {
        Preconditions.checkNotNull(guidePostChunks);
        Preconditions.checkNotNull(decodedGuidePostChunkCache);
        Preconditions.checkArgument(indexOfFirstChunk >= 0);
        Preconditions.checkArgument(indexOfFirstGuidePost >= 0);
        Preconditions.checkArgument(indexOfLastGuidePost >= 0);
        Preconditions.checkArgument(indexOfLastChunk >= indexOfFirstChunk);

        this.guidePostChunks = guidePostChunks;
        this.decodedGuidePostChunkCache = decodedGuidePostChunkCache;
        this.indexOfFirstChunk = indexOfFirstChunk;
        this.indexOfFirstGuidePost = indexOfFirstGuidePost;
        this.indexOfLastChunk = indexOfLastChunk;
        this.indexOfLastGuidePost = indexOfLastGuidePost;

        this.indexOfCurrentGuidePostChunk = this.indexOfFirstChunk;
        this.indexOfCurrentGuidePost = this.indexOfFirstGuidePost;
        this.decodedGuidePostChunk = decodedGuidePostChunkCache.get(indexOfCurrentGuidePostChunk, true);
    }

    private GuidePost getGuidePost() {
        byte[] key = decodedGuidePostChunk.getGuidePosts().get(indexOfCurrentGuidePost);
        GuidePostEstimation estimation = guidePostChunks.get(
                indexOfCurrentGuidePostChunk).getEstimation(indexOfCurrentGuidePost);
        return new GuidePost(key, estimation);
    }

    @Override
    public boolean hasNext() {
        return indexOfCurrentGuidePostChunk < indexOfLastChunk || indexOfCurrentGuidePost <= indexOfLastGuidePost;
    }

    @Override
    public GuidePost next() {
        GuidePost guidePost = getGuidePost();

        indexOfCurrentGuidePost++;
        if (indexOfCurrentGuidePost == guidePostChunks.get(indexOfCurrentGuidePostChunk).getGuidePostsCount()
                && indexOfCurrentGuidePostChunk < indexOfLastChunk) {
            indexOfCurrentGuidePostChunk++;
            indexOfCurrentGuidePost = 0;
            decodedGuidePostChunk = decodedGuidePostChunkCache.get(indexOfCurrentGuidePostChunk, true);
        }

        return guidePost;
    }

    @Override
    public void remove() {
        assert false; // Should never be called.
    }

    public GuidePost last() {
        if (indexOfCurrentGuidePostChunk != indexOfLastChunk) {
            indexOfCurrentGuidePostChunk = indexOfLastChunk;
            decodedGuidePostChunk = decodedGuidePostChunkCache.get(indexOfLastChunk, true);
        }

        indexOfCurrentGuidePost = indexOfLastGuidePost;
        GuidePost guidePost = getGuidePost();
        indexOfCurrentGuidePost++;

        return guidePost;
    }
}

/**
 *  A class that holds the guide posts of a tenant or a table. It also allows combining
 *  the guide posts of different tenants when the GuidePostsInfo is formed for a table.
 */
public class GuidePostsInfo {
    private static class InnerRangeQueryResult {
        /**
         * The accumulated estimation info within the search range
         */
        private final GuidePostEstimation totalEstimation;

        /**
         * The iterator for the guide posts within the search range
         */
        private final GuidePostIterator guidePostIterator;

        public InnerRangeQueryResult(GuidePostEstimation totalEstimation, GuidePostIterator guidePostIterator) {
            this.totalEstimation = totalEstimation;
            this.guidePostIterator = guidePostIterator;
        }

        public GuidePostEstimation getTotalEstimation() {
            return totalEstimation;
        }

        public GuidePostIterator getGuidePostIterator() {
            return guidePostIterator;
        }
    }

    private static class GuidePostLocation {
        /**
         * The node's index in the tree which is in the representation of an array
         */
        private final int treeNodeIndex;

        /**
         * The index of the guidepost chunk containing the point
         */
        private final int guidePostChunkIndex;

        /**
         * The local index (Zero-based) of the guidepost containing the point
         */
        private final int guidePostLocalIndex;

        /**
         * Cached hash code for this object
         */
        private int memoizedHashCode = 0;

        public GuidePostLocation(int treeNodeIndex, int guidePostChunkIndex, int guidePostLocalIndex) {
            this.treeNodeIndex = treeNodeIndex;
            this.guidePostChunkIndex = guidePostChunkIndex;
            this.guidePostLocalIndex = guidePostLocalIndex;
        }

        public int getTreeNodeIndex() {
            return treeNodeIndex;
        }

        public int getGuidePostChunkIndex() {
            return guidePostChunkIndex;
        }

        public int getGuidePostLocalIndex() {
            return guidePostLocalIndex;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof GuidePostLocation)) {
                return false;
            }

            GuidePostLocation that = (GuidePostLocation)o;
            return this.treeNodeIndex == that.treeNodeIndex &&
                    this.guidePostChunkIndex == that.guidePostChunkIndex &&
                    this.guidePostLocalIndex == that.guidePostLocalIndex;
        }

        @Override
        public int hashCode() {
            if (memoizedHashCode != 0) {
                return memoizedHashCode;
            }

            final int prime = 31;
            int result = 1;
            result = prime * result + this.treeNodeIndex;
            result = prime * result + this.guidePostChunkIndex;
            result = prime * result + this.guidePostLocalIndex;
            memoizedHashCode = result;

            return result;
        }
    }

    private static class GuidePostTreeNode {
        /**
         * The key range of the guide posts that this node covers
         */
        private final KeyRange keyRange;

        /**
         * The accumulated estimation info of the guide posts that this node covers
         */
        private final GuidePostEstimation totalEstimation;

        public GuidePostTreeNode(KeyRange keyRange, GuidePostEstimation totalEstimation) {
            this.keyRange = keyRange;
            this.totalEstimation = totalEstimation;
        }

        public KeyRange getKeyRange() {
            return this.keyRange;
        }

        public GuidePostEstimation getTotalEstimation() {
            return this.totalEstimation;
        }

        /**
         * Merge the two child tree nodes into this node which contains the merged key range
         * and the "sum" of the estimation info of the child nodes.
         * @param left
         * @param right
         * @return the parent node of the given nodes
         */
        public static GuidePostTreeNode merge(GuidePostTreeNode left, GuidePostTreeNode right) {
            KeyRange keyRange = KeyRange.getKeyRange(
                    left.getKeyRange().getLowerRange(), left.getKeyRange().isLowerInclusive(),
                    right.getKeyRange().getUpperRange(), right.getKeyRange().isUpperInclusive());

            GuidePostEstimation estimation = new GuidePostEstimation();
            estimation.merge(left.getTotalEstimation());
            estimation.merge(right.getTotalEstimation());

            return new GuidePostTreeNode(keyRange, estimation);
        }
    }

    private static final class GuidePostTreeLeaf extends GuidePostTreeNode {
        /**
         * The index of the guide post chunk in the chunk array.
         */
        private int guidePostChunkIndex;

        public GuidePostTreeLeaf(int guidePostChunkIndex, KeyRange keyRange,
                GuidePostEstimation totalEstimation) {
            super(keyRange, totalEstimation);

            this.guidePostChunkIndex = guidePostChunkIndex;
        }

        public int getGuidePostChunkIndex() {
            return this.guidePostChunkIndex;
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(GuidePostsInfo.class);

    public final static GuidePostsInfo NO_GUIDEPOST =
            new GuidePostsInfo(null, 0, Collections.<GuidePostChunk> emptyList()) {
                @Override
                public String toString() {
                    GuidePostsKey key = getGuidePostsKey();
                    return "Guide Post Info {" + (key != null ? key.toString() : "GuidePostsKey[null]") + "; No Guide Post.}";
                }
            };

    private final static BytesComparator comparator = ScanUtil.getComparator(true, SortOrder.ASC);;

    /**
     * Guide Posts key
     */
    private GuidePostsKey guidePostsKey;

    /**
     * The guide post chunks. The guide posts held by this tree are split into chunks
     */
    private final List<GuidePostChunk> guidePostChunks;

    /**
     * The guide posts info is organized in a tree. The tree is in the representation of array
     */
    private final GuidePostTreeNode[] nodes;

    /**
     * The size of the tree
     */
    private final int treeSize;

    /**
     * Estimation of byte size of this instance contributes to cache
     */
    private final int estimatedSize;

    /**
     * The total count of guide posts collected
     */
    private final int guidePostsCount;

    /**
     * Construct the Variant Segment Tree (https://salesforce.quip.com/taWiALFmhquO)
     * from the given guide post chunks
     * @param guidePostChunks -- the guide post chunks from which the tree is built
     */
    public GuidePostsInfo(GuidePostsKey guidePostsKey, int guidePostsCount, List<GuidePostChunk> guidePostChunks) {
        this.guidePostsKey = guidePostsKey;
        this.guidePostChunks = guidePostChunks;
        this.guidePostsCount = guidePostsCount;

        if (this.guidePostChunks != null && this.guidePostChunks.size() > 0) {
            int n = this.guidePostChunks.size();
            int height = (int) (Math.ceil(Math.log(n) / Math.log(2))); // The height of this tree
            this.treeSize = 2 * (int) Math.pow(2, height) - 1;
            this.nodes = new GuidePostTreeNode[this.treeSize];

            // We calculate how much this instance contributes to cache approximately,
            // so we only count the size of guide post chunk array.
            // The contribution to cache from other fields are ignorable.
            int byteCount = 0;
            for (GuidePostChunk chunk : this.guidePostChunks) {
                byteCount += chunk.getEstimatedSize();
            }
            this.estimatedSize = byteCount;

            Construct(this.guidePostChunks, 0, n - 1, 0);
        } else {
            this.treeSize = 0;
            this.nodes = null;
            this.estimatedSize = 0;
        }
    }

    /**
     * Recursively construct the tree based on the given guide post chunks.
     * @param guidePostChunks -- the guide post chunks
     * @param start -- the start index in the guide post chunks (inclusive)
     * @param end -- the end index in the guide post chunks (inclusive)
     * @param index -- the index of the current node in the array of the tree nodes.
     *                 The current node is the root node of the sub-tree covering
     *                 guide post chunks [start ... end]
     */
    private GuidePostTreeNode Construct(List<GuidePostChunk> guidePostChunks, int start, int end, int index) {
        if (start == end) {
            // There is only one element in the guide post chunk array, store it to the tree node
            nodes[index] = new GuidePostTreeLeaf(start, guidePostChunks.get(start).getKeyRange(),
                    guidePostChunks.get(start).getTotalEstimation());
        }
        else {
            // There are more than one element in the guide post chunk array,
            // so construct the left and right sub-trees recursively in post-order,
            // add store the "sum" to this tree node.
            int mid = start + (end - start) / 2;
            GuidePostTreeNode left = Construct(guidePostChunks, start, mid, index * 2 + 1);
            GuidePostTreeNode right = Construct(guidePostChunks, mid + 1, end, index * 2 + 2);
            nodes[index] = GuidePostTreeNode.merge(left, right);
        }

        return nodes[index];
    }

    private int getLowestCommonAncestor(int index, KeyRange keyRange) {
        if (index > this.treeSize - 1 || index < 0 || nodes[index] == null) {
            return -1;
        }

        if (! nodes[index].getKeyRange().contains(keyRange)) {
            return -1;
        }

        // If the left sub-tree covers the given range, return its index
        int leftAnswer = getLowestCommonAncestor(index * 2 + 1, keyRange);
        if (leftAnswer >= 0) {
            return leftAnswer;
        }

        // If the right sub-tree covers the given range, return its index
        int rightAnswer = getLowestCommonAncestor(index * 2 + 2, keyRange);
        if (rightAnswer >= 0) {
            return rightAnswer;
        }

        // Return this node as the answer
        return index;
    }

    private GuidePostLocation drawHalfSkyline(int index, byte[] key, boolean isInclusive, boolean isLowerBound,
            DecodedGuidePostChunkCache decodedGuidePostChunkCache, Set<Integer> skyline) {
        if (index > this.treeSize - 1 || index < 0 || nodes[index] == null) {
            return null;
        }

        KeyRange keyRange = nodes[index].getKeyRange();

        if (isLowerBound) {
            // If this node's lower boundary is on the right side of the given lower boundary,
            // this node's range doesn't contain the given lower boundary, but this node is
            // a critical point on the skyline.
            int lowerToLower = keyRange.compareLowerRange(key, 0, key.length, isInclusive);
            if (lowerToLower > 0) {
                skyline.add(index);
                return null;
            }

            // This node's range doesn't contain the given lower boundary in the following two cases:
            // a. This node's upper boundary is on the left side of the given lower boundary
            // b. This node's upper boundary is equal to the given lower boundary but both are exclusive
            int upperToLower = keyRange.compareUpperToLowerBound(key, 0, key.length, isInclusive, this.comparator);
            if (upperToLower < 0) {
                return null;
            }
        } else {
            // If this node's upper boundary is on the left side of the given upper boundary,
            // this node's range doesn't contain the given upper boundary, but this node is
            // a critical point on the skyline.
            int upperToUpper = keyRange.compareUpperRange(key, 0, key.length, isInclusive);
            if (upperToUpper < 0) {
                skyline.add(index);
                return null;
            }

            // This node's range contains the given upper boundary in the following two cases:
            // a. This node's lower boundary is on the left side of the given upper boundary
            // b. This node's lower boundary is equal to the given upper boundary but both are inclusive
            int lowerToUpper = keyRange.compareLowerToUpperBound(key, 0, key.length, isInclusive, this.comparator);
            if (lowerToUpper > 0) {
                return null;
            }
        }

        if (nodes[index] instanceof GuidePostTreeLeaf) {
            GuidePostTreeLeaf leaf = (GuidePostTreeLeaf)(nodes[index]);
            DecodedGuidePostChunk decodedChunk =
                    decodedGuidePostChunkCache.get(leaf.getGuidePostChunkIndex(), false);
            int guidePostLocalIndex = decodedChunk.locateGuidePost(key, isLowerBound);
            return new GuidePostLocation(index, leaf.getGuidePostChunkIndex(), guidePostLocalIndex);
        }

        // Search in the left sub-tree
        GuidePostLocation leftAnswer = drawHalfSkyline(
                index * 2 + 1, key, isInclusive, isLowerBound, decodedGuidePostChunkCache, skyline);

        // Search in the right sub-tree
        GuidePostLocation rightAnswer = drawHalfSkyline(
                index * 2 + 2, key, isInclusive, isLowerBound, decodedGuidePostChunkCache, skyline);

        if (isLowerBound) {
            // Both the leftmost guide post and its next neighbor can be leaf node and non-null value,
            // return the leftmost guide post and add the neighbor to the skyline.
            if (leftAnswer != null && rightAnswer != null) {
                skyline.add(rightAnswer.getTreeNodeIndex());
            }
            return leftAnswer != null ? leftAnswer : rightAnswer;
        } else {
            // Both the rightmost guide post and its previous neighbor can be leaf node and non-null value,
            // return the rightmost guide post and add the neighbor to the skyline.
            if (leftAnswer != null && rightAnswer != null) {
                skyline.add(leftAnswer.getTreeNodeIndex());
            }
            return rightAnswer != null ? rightAnswer : leftAnswer;
        }
    }

    private InnerRangeQueryResult rangeQuery(int index, KeyRange keyRange,
            DecodedGuidePostChunkCache decodedGuidePostChunkCache) {
        if (index > this.treeSize - 1 || index < 0 || nodes[index] == null) {
            return null;
        }

        int lowestCommonAncestor = getLowestCommonAncestor(index, keyRange);
        if (lowestCommonAncestor < 0) {
            return null;
        }

        GuidePostIterator guidePostIterator;
        GuidePostEstimation totalEstimation = new GuidePostEstimation();

        if (nodes[lowestCommonAncestor] instanceof GuidePostTreeLeaf) {
            // short cut for leaf node
            GuidePostTreeLeaf leaf = (GuidePostTreeLeaf)(nodes[lowestCommonAncestor]);
            int guidePostChunkIndex = leaf.getGuidePostChunkIndex();
            GuidePostChunk chunk = guidePostChunks.get(guidePostChunkIndex);
            DecodedGuidePostChunk decodedChunk =
                    decodedGuidePostChunkCache.get(guidePostChunkIndex, false);

            int guidePostLocalIndexForLower = decodedChunk.locateGuidePost(keyRange.getLowerRange(), true);
            int guidePostLocalIndexForUpper = decodedChunk.locateGuidePost(keyRange.getUpperRange(), false);
            assert (guidePostLocalIndexForLower >= 0);
            assert (guidePostLocalIndexForUpper >= 0);

            GuidePostEstimation estimation = chunk.getTotalEstimation(
                    guidePostLocalIndexForLower, guidePostLocalIndexForUpper + 1);
            totalEstimation.merge(estimation);

            guidePostIterator = new GuidePostIterator(
                    guidePostChunks, decodedGuidePostChunkCache,
                    guidePostChunkIndex, guidePostLocalIndexForLower,
                    guidePostChunkIndex, guidePostLocalIndexForUpper);
        } else {
            boolean adjustedLeftmostGuidePostLocation = false;
            Set<Integer> skyline = Sets.newHashSet();

            // The leftmost guide post must be located at the left sub-tree, or it is the first guide post
            // in the right sub-tree.
            GuidePostLocation leftmostGuidePostLocation = drawHalfSkyline(lowestCommonAncestor * 2 + 1,
                    keyRange.getLowerRange(), keyRange.isLowerInclusive(), true, decodedGuidePostChunkCache, skyline);
            GuidePostChunk chunk = guidePostChunks.get(leftmostGuidePostLocation.getGuidePostChunkIndex());
            if (chunk.getGuidePostsCount() == leftmostGuidePostLocation.guidePostLocalIndex) {
                // This only happens when the query key range's lower boundary is equal to the node range's
                // upper boundary and both are inclusive. In this case, we should use the next guide post
                // as the leftmost guide post.
                assert (keyRange.isLowerInclusive());
                leftmostGuidePostLocation = new GuidePostLocation(
                        -1, leftmostGuidePostLocation.getGuidePostChunkIndex() + 1, 0);
                chunk = guidePostChunks.get(leftmostGuidePostLocation.getGuidePostChunkIndex());
                adjustedLeftmostGuidePostLocation = true;
            }

            // The rightmost guide post must be located at the right sub-tree
            GuidePostLocation rightmostGuidePostLocation = drawHalfSkyline(lowestCommonAncestor * 2 + 2,
                    keyRange.getUpperRange(), keyRange.isUpperInclusive(), false, decodedGuidePostChunkCache, skyline);

            if (leftmostGuidePostLocation.getGuidePostChunkIndex() != rightmostGuidePostLocation.getGuidePostChunkIndex()) {
                if (! adjustedLeftmostGuidePostLocation) {
                    // In this case, we should calculate the estimation from the leftmost guide post chunk;
                    // otherwise, the estimation has been covered by the skyline.
                    GuidePostEstimation estimation = chunk.getTotalEstimationToEnd(
                            leftmostGuidePostLocation.getGuidePostLocalIndex());
                    totalEstimation.merge(estimation);
                }
                chunk = guidePostChunks.get(rightmostGuidePostLocation.getGuidePostChunkIndex());
                GuidePostEstimation estimation = chunk.getTotalEstimationFromStart(
                        rightmostGuidePostLocation.getGuidePostLocalIndex() + 1);
                totalEstimation.merge(estimation);
            } else {
                // We don't need to consider the case of "adjustedLeftmostGuidePostLocation == true" here,
                // because the skyline won't include the the leftmost/rightmost guide post chunk.
                GuidePostEstimation estimation = chunk.getTotalEstimation(
                        leftmostGuidePostLocation.getGuidePostLocalIndex(), rightmostGuidePostLocation.guidePostLocalIndex + 1);
                totalEstimation.merge(estimation);
            }

            // Calculate accumulative estimation from skyline
            for (Integer i : skyline) {
                totalEstimation.merge(nodes[i].getTotalEstimation());
            }

            guidePostIterator = new GuidePostIterator(guidePostChunks, decodedGuidePostChunkCache,
                    leftmostGuidePostLocation.getGuidePostChunkIndex(), leftmostGuidePostLocation.getGuidePostLocalIndex(),
                    rightmostGuidePostLocation.getGuidePostChunkIndex(), rightmostGuidePostLocation.getGuidePostLocalIndex());
        }

        // If there is no last update timestamp, which happens when the query only hits the last chunk
        // (the ending chunk), then uses the root's timestamp.
        if (totalEstimation.getTimestamp() == GuidePostEstimation.MAX_TIMESTAMP) {
            totalEstimation.setTimestamp(nodes[0].getTotalEstimation().getTimestamp());
        }

        return new InnerRangeQueryResult(totalEstimation, guidePostIterator);
    }

    private Pair<List<KeyRange>, GuidePostEstimation> rangeQueries(
            List<KeyRange> queryKeyRanges, boolean generateParallelScanRanges) {
        Preconditions.checkArgument(queryKeyRanges != null && queryKeyRanges.size() > 0);

        List<KeyRange> parallelScanRanges = Lists.newArrayListWithExpectedSize(1);
        GuidePostEstimation totalEstimation = new GuidePostEstimation();

        if (queryKeyRanges.size() == 1 &&
                KeyRange.isDegenerate(queryKeyRanges.get(0).getLowerRange(), queryKeyRanges.get(0).getUpperRange())) {
            return new Pair(parallelScanRanges, totalEstimation);
        }

        int lowestCommonAncestor = 0;
        if (queryKeyRanges.size() > 1) {
            // Only need to optimize for the case that we have multiple query key ranges in this region
            lowestCommonAncestor = getLowestCommonAncestor(0,
                    KeyRange.getKeyRange(queryKeyRanges.get(0).getLowerRange(), queryKeyRanges.get(0).isLowerInclusive(),
                            queryKeyRanges.get(queryKeyRanges.size() - 1).getUpperRange(),
                            queryKeyRanges.get(queryKeyRanges.size() - 1).isUpperInclusive()));
            assert (lowestCommonAncestor >= 0);
        }

        byte[] startKey = null;
        GuidePost theCurrentGuidePost = null;
        DecodedGuidePostChunkCache decodedGuidePostChunkCache = new DecodedGuidePostChunkCache(this.guidePostChunks);

        for (int i = 0; i < queryKeyRanges.size(); i++) {
            if (startKey == null) {
                startKey = queryKeyRanges.get(i).getLowerRange();
            }

            if (theCurrentGuidePost == null) {
                KeyRange queryKeyRange = KeyRange.getKeyRange(startKey, true,
                        queryKeyRanges.get(i).getUpperRange(), queryKeyRanges.get(i).isUpperInclusive());
                InnerRangeQueryResult innerRangeQueryResult =
                        rangeQuery(lowestCommonAncestor, queryKeyRange, decodedGuidePostChunkCache);
                assert (innerRangeQueryResult != null);
                assert (innerRangeQueryResult.getGuidePostIterator().hasNext());

                totalEstimation.merge(innerRangeQueryResult.getTotalEstimation());

                if (generateParallelScanRanges) {
                    while (innerRangeQueryResult.getGuidePostIterator().hasNext()) {
                        theCurrentGuidePost = innerRangeQueryResult.getGuidePostIterator().next();
                        if (theCurrentGuidePost.getGuidePostKey() == KeyRange.UNBOUND) {
                            break;
                        }

                        int compareQueryUpperRangeToGuidePost =
                                queryKeyRanges.get(i).compareUpperRange(
                                        theCurrentGuidePost.getGuidePostKey(), 0,
                                        theCurrentGuidePost.getGuidePostKey().length, true);
                        if (compareQueryUpperRangeToGuidePost >= 0) {
                            parallelScanRanges.add(KeyRange.getKeyRange(startKey, true,
                                    theCurrentGuidePost.getGuidePostKey(), false));
                            startKey = theCurrentGuidePost.getGuidePostKey();
                        }
                        else {
                            // This is the first guide post covers the query range and the last guide post in the iterator.
                            // Do nothing.
                        }
                    }
                } else {
                    theCurrentGuidePost = innerRangeQueryResult.getGuidePostIterator().last();
                }

                if (theCurrentGuidePost.getGuidePostKey() == KeyRange.UNBOUND) {
                    // This should only happen when all the remaining query key ranges, including the current one,
                    // hit the ending chunk at the end of chunk list, and they all have been handled in this case.
                    break;
                }
            }
            else {
                int compareQueryUpperRangeToGuidePost =
                        queryKeyRanges.get(i).compareUpperRange(
                                theCurrentGuidePost.getGuidePostKey(), 0,
                                theCurrentGuidePost.getGuidePostKey().length, true);
                assert (compareQueryUpperRangeToGuidePost != 0); // Because upper range of the query key range is always false
                if (compareQueryUpperRangeToGuidePost < 0) {
                    // The current query key range is within the current guide post.
                    // Move forward and check the next query key range.
                    continue;
                } else {
                    // In the case that the current guide post is left to query key range's upper range
                    int compareQueryLowerRangeToGuidePost = queryKeyRanges.get(i).compareLowerToUpperBound(
                            theCurrentGuidePost.getGuidePostKey(), comparator);
                    if (compareQueryLowerRangeToGuidePost < 0) {
                        // In the case that the current guide post is between query lower range and query upper range.
                        if (generateParallelScanRanges) {
                            parallelScanRanges.add(KeyRange.getKeyRange(startKey, true,
                                    theCurrentGuidePost.getGuidePostKey(), false));
                        }
                        startKey = theCurrentGuidePost.getGuidePostKey();
                    } else {
                        // In the case that query key lower range is at or right to the current guide post
                        if (generateParallelScanRanges) {
                            parallelScanRanges.add(KeyRange.getKeyRange(startKey, true,
                                    queryKeyRanges.get(i - 1).getUpperRange(), false));
                        }
                        startKey = queryKeyRanges.get(i).getLowerRange();
                    }

                    theCurrentGuidePost = null;
                    i--; // step back to continue with the current query range but new start key
                }
            }
        }

        if (generateParallelScanRanges && startKey != null) {
            assert (! queryKeyRanges.get(queryKeyRanges.size() - 1).isUpperInclusive());
            byte[] endKey = queryKeyRanges.get(queryKeyRanges.size() - 1).getUpperRange();
            parallelScanRanges.add(KeyRange.getKeyRange(startKey, true, endKey, false));
        }

        return new Pair(parallelScanRanges, totalEstimation);
    }

    /**
     * Given the query key Ranges, calculate and return the accumulative estimation from the minimal guide post
     * set which covers the query key ranges.
     *
     * @param queryKeyRanges
     * @return
     */
    public GuidePostEstimation getEstimationOnly(List<KeyRange> queryKeyRanges) {
        Pair<List<KeyRange>, GuidePostEstimation> result =
                rangeQueries(queryKeyRanges, false);
        return result.getSecond();
    }

    /**
     * Given the query key Ranges, group the query key ranges by guide post, and for each group return the scan range
     * which is the smallest key range covering the group. If a query key range crosses guide post boundary, it will
     * be split into different groups.
     *
     * For example, assume we have guide posts with keys {10, 20, 30, 50}, given the query key ranges [0, 1), [3, 5),
     * [7, 30), [45, 100), the scan ranges will be [0, 10), [10, 20), [20, 30), [45, 50), [50, UNBOUND)
     *
     * @param queryKeyRanges
     * @return
     */
    public Pair<List<KeyRange>, GuidePostEstimation> generateParallelScanRanges(List<KeyRange> queryKeyRanges) {
        return rangeQueries(queryKeyRanges, true);
    }

    public int getEstimatedSize() {
        return estimatedSize;
    }

    public List<GuidePostChunk> getGuidePostChunks() {
        return guidePostChunks;
    }

    public int getGuidePostsCount() {
        return guidePostsCount;
    }

    public GuidePostEstimation getTotalEstimation() {
        return (nodes != null && nodes.length > 0) ? nodes[0].getTotalEstimation() : new GuidePostEstimation();
    }

    public GuidePostsKey getGuidePostsKey() {
        return guidePostsKey;
    }

    public void setGuidePostsKey(GuidePostsKey guidePostsKey) {
        this.guidePostsKey = guidePostsKey;
    }

    @Override
    public String toString() {
        GuidePostEstimation totalEstimation = getTotalEstimation();
        GuidePostsKey key = getGuidePostsKey();
        return "Guide Post Info {" +
                (key != null ? key.toString() : "GuidePostsKey[null]") +
                "; Estimation: " + totalEstimation.toString() +
                "; Guide Posts Count: " + getGuidePostsCount() +
                "; Estimated Size (Cache): " + getEstimatedSize() +
                "}";
    }
}