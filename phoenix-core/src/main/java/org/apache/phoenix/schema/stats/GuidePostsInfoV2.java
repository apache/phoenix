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

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.ScanUtil.BytesComparator;
import org.apache.phoenix.query.KeyRange;

class GuidePostTreeNode {
    /**
     * The key range of the guide posts that this node covers
     */
    private KeyRange keyRange;

    /**
     * The accumulated estimation info of the guide posts that this node covers
     */
    private GuidePostEstimation accumulatedEstimation;

    public GuidePostTreeNode() {}

    public GuidePostTreeNode(KeyRange keyRange, GuidePostEstimation accumulatedEstimation) {
        this.keyRange = keyRange;
        this.accumulatedEstimation = accumulatedEstimation;
    }

    public KeyRange getKeyRange() {
        return this.keyRange;
    }

    public GuidePostEstimation getAccumulatedEstimation() {
        return this.accumulatedEstimation;
    }

    public void setKeyRange(KeyRange keyRange) {
        this.keyRange = keyRange;
    }

    public void setAccumulatedEstimation(GuidePostEstimation accumulatedEstimation) {
        this.accumulatedEstimation = accumulatedEstimation;
    }

    /**
     * Merge the two child tree nodes into this node which contains the merged key range
     * and the "sum" of the estimation info of the child nodes.
     * @param left
     * @param right
     * @return the parent node of the given nodes
     */
    public static GuidePostTreeNode merge(GuidePostTreeNode left, GuidePostTreeNode right) {
        GuidePostTreeNode node = new GuidePostTreeNode();
        KeyRange tempKeyRange = KeyRange.getKeyRange(left.getKeyRange().getLowerRange(), left.getKeyRange().isLowerInclusive(),
                right.getKeyRange().getUpperRange(), right.getKeyRange().isUpperInclusive());

        GuidePostEstimation tempEstimation = GuidePostEstimation.merge(
                left.getAccumulatedEstimation(), right.getAccumulatedEstimation());

        node.setKeyRange(tempKeyRange);
        node.setAccumulatedEstimation(tempEstimation);

        return node;
    }
}

final class GuidePostTreeLeafNode extends GuidePostTreeNode {
    /**
     * The index of the guide post chunk in the chunk array. The chunk array is on the tree level.
     */
    private int guidePostChunkIndex;

    public GuidePostTreeLeafNode() {
        guidePostChunkIndex = GuidePostChunk.INVALID_GUIDEPOST_CHUNK_INDEX;
    }

    public GuidePostTreeLeafNode(int guidePostChunkIndex, KeyRange keyRange,
            GuidePostEstimation accumulatedEstimation) {
        super(keyRange, accumulatedEstimation);

        this.guidePostChunkIndex = guidePostChunkIndex;
    }

    public int getGuidePostChunkIndex() {
        return this.guidePostChunkIndex;
    }
}

final class GuidePostChunk {
    public static final int INVALID_GUIDEPOST_CHUNK_INDEX = -1;

    /**
     * The total count of guide posts covered by this chunk
     */
    private int guidePostsCount;

    /**
     * The key range of the guide posts in this chunk
     */
    private KeyRange keyRange;

    /**
     * The start index of the guide posts in this chunk, which is the global index
     * of this guide post in all the guide posts across all the chunks.
     */
    private int globalStartIndex;

    /**
     * The guide posts .
     */
    private ImmutableBytesWritable guidePosts;

    /**
     * Maximum length of a guide post collected
     */
    private int maxLength;

    /**
     * The estimation info of each guide post traversed
     */
    private GuidePostEstimation[] estimations;

    /**
     * The accumulated estimation info of the guide posts in this chunk
     */
    private GuidePostEstimation accumulatedEstimation;

    public int getGuidePostsCount() {
        return guidePostsCount;
    }

    public KeyRange getKeyRange() {
        return keyRange;
    }

    public int getGlobalStartIndex() {
        return globalStartIndex;
    }

    public void setKeyRange(KeyRange keyRange) {
        this.keyRange = keyRange;
    }

    public ImmutableBytesWritable getGuidePosts() {
        return guidePosts;
    }

    public int getMaxLength() {
        return maxLength;
    }

    public GuidePostEstimation[] getEstimations() {
        return estimations;
    }

    public GuidePostEstimation getAccumulatedEstimation() {
        return accumulatedEstimation;
    }

    public void setAccumulatedEstimation(GuidePostEstimation accumulatedEstimation) {
        this.accumulatedEstimation = accumulatedEstimation;
    }

    public int getEstimatedSize() {
        return 0;
    }
}

public final class GuidePostsInfoV2 {
    public final class RangeQueryResult {
        /**
         * The accumulated estimation info within the search range
         */
        private final GuidePostEstimation accumulatedEstimation;

        /**
         * The parallel scan ranges. If we use stats for parallelization,
         * the granularity of parallel scan is one guide post; otherwise, one region.
         */
        private final List<List<KeyRange>> parallelScanRangesGroupedByRegion;

        public RangeQueryResult(GuidePostEstimation accumulatedEstimation, List<List<KeyRange>> parallelScanRangesGroupedByRegion) {
            this.accumulatedEstimation = accumulatedEstimation;
            this.parallelScanRangesGroupedByRegion = parallelScanRangesGroupedByRegion;
        }

        public GuidePostEstimation getAccumulatedEstimation() {
            return accumulatedEstimation;
        }

        public List<List<KeyRange>> getParallelScanRangesGroupedByRegion() {
            return parallelScanRangesGroupedByRegion;
        }
    }

    private static class GuidePost {
        public static final int INVALID_GUIDEPOST_INDEX = -1;

        private final int globalIndex;
        private final byte[] guidePostKey;
        private final GuidePostEstimation estimation;

        public GuidePost(int globalIndex, byte[] guidePostKey, GuidePostEstimation estimation) {
            this.globalIndex = globalIndex;
            this.guidePostKey = guidePostKey;
            this.estimation = estimation;
        }

        public int getGlobalIndex() {
            return globalIndex;
        }

        public byte[] getGuidePostKey() {
            return guidePostKey;
        }

        public GuidePostEstimation getEstimation() {
            return estimation;
        }
    }

    private static class GuidePostIterator implements Iterator<GuidePost> {
        /**
         * The guide post chunks. The guide posts held by this tree are split into chunks
         */
        private final ArrayList<GuidePostChunk> guidePostChunks;

        /**
         * The index of the first guide post in the search range, which is the global index
         * of this guide post in all the guide posts across all the chunks.
         */
        private final int globalIndexOfTheFirstGuidePost;

        /**
         * The index of the last guide post in the search range, which is the global index
         * of this guide post in all the guide posts across all the chunks.
         */
        private final int globalIndexOfTheLastGuidePost;

        /**
         * The start index of the returned chunks
         */
        private final int startIndexOfReturnedChunks;

        /**
         * The end index of the returned chunks
         */
        private final int endIndexOfReturnedChunks;

        /**
         * The decoded guide posts in the first returned chunk
         */
        private final ArrayList<byte[]> cachedGuidePostsOfTheFirstReturnedChunk;

        /**
         * The decoded guide posts in the last returned chunk
         */
        private final ArrayList<byte[]> cacheGuidePostsOfTheLastReturnedChunk;

        private int globalIndexOfTheCurrentGuidePost;

        public GuidePostIterator(ArrayList<GuidePostChunk> guidePostChunks,
                int globalIndexOfTheFirstGuidePost, int globalIndexOfTheLastGuidePost,
                int startIndexOfReturnedChunks, ArrayList<byte[]> cachedGuidePostsOfTheFirstReturnedChunk,
                int endIndexOfReturnedChunks, ArrayList<byte[]> cacheGuidePostsOfTheLastReturnedChunk) {
            this.guidePostChunks = guidePostChunks;
            this.globalIndexOfTheFirstGuidePost = globalIndexOfTheFirstGuidePost;
            this.globalIndexOfTheLastGuidePost = globalIndexOfTheLastGuidePost;
            this.startIndexOfReturnedChunks = startIndexOfReturnedChunks;
            this.endIndexOfReturnedChunks = endIndexOfReturnedChunks;
            this.cachedGuidePostsOfTheFirstReturnedChunk = cachedGuidePostsOfTheFirstReturnedChunk;
            this.cacheGuidePostsOfTheLastReturnedChunk = cacheGuidePostsOfTheLastReturnedChunk;

            this.globalIndexOfTheCurrentGuidePost = GuidePost.INVALID_GUIDEPOST_INDEX;
        }

        @Override
        public boolean hasNext() {
            return globalIndexOfTheCurrentGuidePost < globalIndexOfTheLastGuidePost;
        }

        @Override
        public GuidePost next() {
            // BINSHI-TODO
            return new GuidePost(GuidePost.INVALID_GUIDEPOST_INDEX, null, null);
        }

        @Override
        public void remove() {
            assert false; // Should never be called.
        }

        public GuidePost last() {
            globalIndexOfTheCurrentGuidePost = globalIndexOfTheLastGuidePost;

            if (cacheGuidePostsOfTheLastReturnedChunk == null) {
                return null;
            }

            int localIndex = globalIndexOfTheLastGuidePost - guidePostChunks.get(endIndexOfReturnedChunks).getGlobalStartIndex();
            return new GuidePost(globalIndexOfTheCurrentGuidePost, cacheGuidePostsOfTheLastReturnedChunk.get(localIndex),
                    guidePostChunks.get(endIndexOfReturnedChunks).getEstimations()[localIndex]);
        }
    }

    private static class InnerRangeQueryResult {
        /**
         * The accumulated estimation info for the given search range
         */
        private final GuidePostEstimation accumulatedEstimation;

        private final GuidePostIterator guidePostIterator;

        public InnerRangeQueryResult(GuidePostEstimation accumulatedEstimation, GuidePostIterator guidePostIterator) {
            this.accumulatedEstimation = accumulatedEstimation;
            this.guidePostIterator = guidePostIterator;
        }

        public GuidePostEstimation getAccumulatedEstimation() {
            return accumulatedEstimation;
        }

        public GuidePostIterator getGuidePostIterator() {
            return guidePostIterator;
        }
    }

    private static BytesComparator comparator;

    /**
     * The guide post chunks. The guide posts held by this tree are split into chunks
     */
    private final ArrayList<GuidePostChunk> guidePostChunks;

    /**
     * The tree is in the representation of array
     */
    private final GuidePostTreeNode[] nodes;

    /**
     * The size of the tree
     */
    private final int treeSize;

    /**
     * Construct the Variant Segment Tree (https://salesforce.quip.com/taWiALFmhquO)
     * from the given guide post chunks
     * @param guidePostChunks -- the guide post chunks from which the tree is built
     */
    public GuidePostsInfoV2(ArrayList<GuidePostChunk> guidePostChunks) {
        this.guidePostChunks = guidePostChunks;
        this.comparator = ScanUtil.getComparator(true, SortOrder.ASC);

        int n = guidePostChunks.size();
        int height = (int)(Math.ceil(Math.log(n) / Math.log(2))); // The height of this tree
        this.treeSize = 2 * (int)Math.pow(2, height) - 1;
        this.nodes = new GuidePostTreeNode[this.treeSize];

        Construct(this.guidePostChunks, 0, n - 1, 0);
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
    private GuidePostTreeNode Construct(ArrayList<GuidePostChunk> guidePostChunks, int start, int end, int index) {
        if (start == end) {
            // There is only one element in the guide post chunk array, store it to the tree node
            nodes[index] = new GuidePostTreeLeafNode(start, guidePostChunks.get(start).getKeyRange(),
                    guidePostChunks.get(start).getAccumulatedEstimation());
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
        if (index > this.treeSize - 1 || index < 0) {
            return -1;
        }

        // If this node's lower boundary is on the right side of given range's lower boundary,
        // return -1 indicating this node doesn't cover the given range.
        int lowerToLower = KeyRange.compareLowerRange(nodes[index].getKeyRange(), keyRange);
        if (lowerToLower > 0) {
            return -1;
        }

        // If this node's upper boundary is on the left side of given range's upper boundary,
        // return -1 indicating this node doesn't cover the given range.
        int upperToUpper = KeyRange.compareUpperRange(nodes[index].getKeyRange(), keyRange);
        if (upperToUpper < 0) {
            return -1;
        }

        // If the left sub-tree cover the given range, return its index
        int leftAnswer = getLowestCommonAncestor(index * 2 + 1, keyRange);
        if (leftAnswer >= 0) {
            return leftAnswer;
        }

        // If the right sub-tree cover the given range, return its index
        int rightAnswer = getLowestCommonAncestor(index * 2 + 2, keyRange);
        if (rightAnswer >= 0) {
            return rightAnswer;
        }

        // Return this node as the answer
        return index;
    }

    private InnerRangeQueryResult rangeQuery(int index, KeyRange keyRange) {
        if (index > this.treeSize - 1 || index < 0) {
            return null;
        }

        int lowestCommonAncestor = getLowestCommonAncestor(index, keyRange);
        if (lowestCommonAncestor < 0) {
            return null;
        }

        int globalIndexOfTheFirstGuidePost;
        int startIndexOfReturnedChunks;
        ArrayList<byte[]> cacheGuidePostsOfTheLastReturnedChunk) {}

    }

    public GuidePostEstimation getEstimation(List<KeyRange> queryKeyRanges) {
        GuidePostEstimation accumulatedEstimation = new GuidePostEstimation();

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
        boolean startKeyInclusive = false;
        GuidePost theCurrentGuidePost = null;

        for (int i = 0; i < queryKeyRanges.size(); i++) {
            if (startKey == null) {
                startKey = queryKeyRanges.get(i).getLowerRange();
                startKeyInclusive = queryKeyRanges.get(i).isLowerInclusive();
            }

            if (theCurrentGuidePost == null) {
                InnerRangeQueryResult innerRangeQueryResult = rangeQuery(lowestCommonAncestor,
                        KeyRange.getKeyRange(startKey, startKeyInclusive, queryKeyRanges.get(i).getUpperRange(), queryKeyRanges.get(i).isUpperInclusive()));
                assert (innerRangeQueryResult != null);

                accumulatedEstimation.merge(innerRangeQueryResult.getAccumulatedEstimation());

                if (innerRangeQueryResult.getGuidePostIterator().hasNext()) {
                    theCurrentGuidePost = innerRangeQueryResult.getGuidePostIterator().last();
                } else {
                    // This should only happen when all the remaining query key ranges, including the current one,
                    // exceed the upper bound of the guide posts, i.e., the end key of the last guide post. In this case,
                    // there is no guide post estimation for these ranges, but we need to generate scan range for it.
                    break;
                }
            } else {
                int compareQueryUpperRangeToGuidePost = queryKeyRanges.get(i).compareUpperRange(
                        theCurrentGuidePost.getGuidePostKey(), 0, theCurrentGuidePost.getGuidePostKey().length, true);
                if (compareQueryUpperRangeToGuidePost < 0) {
                    continue; // The current query key range is within the current guide post. Move forward and check the next query key range.
                } else if (compareQueryUpperRangeToGuidePost == 0) {
                    startKey = theCurrentGuidePost.getGuidePostKey();
                    startKeyInclusive = true;
                    theCurrentGuidePost = null;
                } else {
                    // In the case that the current guide post is left to query key range's upper range
                    int compareQueryLowerRangeToGuidePost = queryKeyRanges.get(i).compareLowerToUpperBound(
                            theCurrentGuidePost.getGuidePostKey(), comparator);
                    if (compareQueryLowerRangeToGuidePost < 0) {
                        // In the case that the current guide post is within (query lower range, query upper range) with both ends exclusive
                        startKey = theCurrentGuidePost.getGuidePostKey();
                        startKeyInclusive = true;
                    } else {
                        // In the case that query key lower range is at or right to the current guide post
                        startKey = queryKeyRanges.get(i).getLowerRange();
                        startKeyInclusive = queryKeyRanges.get(i).isLowerInclusive();
                    }

                    theCurrentGuidePost = null;
                    i--; // step back to continue with the current query range but new start key
                }
            }
        }

        return accumulatedEstimation;
    }

    public RangeQueryResult rangeQuery(List<KeyRange> regionKeyRanges, List<Pair<Integer, List<KeyRange>>> queryKeyRangesGroupedByRegion) {
        GuidePostEstimation accumulatedEstimation = new GuidePostEstimation();
        List<List<KeyRange>> parallelScanRangesGroupedByRegion = Lists.newArrayListWithCapacity(queryKeyRangesGroupedByRegion.size());

        for (int i = 0; i < queryKeyRangesGroupedByRegion.size(); i++) {
            int regionIndex = queryKeyRangesGroupedByRegion.get(i).getFirst();
            List<KeyRange> queryKeyRanges = queryKeyRangesGroupedByRegion.get(i).getSecond();
            assert (queryKeyRanges.size() > 0);

            int lowestCommonAncestor = 0;
            if (queryKeyRanges.size() > 1) {
                // Only need to optimize for the case that we have multiple query key ranges in this region
                lowestCommonAncestor = getLowestCommonAncestor(0,
                        KeyRange.getKeyRange(queryKeyRanges.get(0).getLowerRange(), queryKeyRanges.get(0).isLowerInclusive(),
                                queryKeyRanges.get(queryKeyRanges.size() - 1).getUpperRange(),
                                queryKeyRanges.get(queryKeyRanges.size() - 1).isUpperInclusive()));
                assert (lowestCommonAncestor >= 0);
            }

            List<KeyRange> parallelScanRanges = Lists.newArrayListWithExpectedSize(1);

            byte[] startKey = null;
            boolean startKeyInclusive = false;
            GuidePost theCurrentGuidePost = null;
            for (int j = 0; j < queryKeyRanges.size(); j++) {
                if (startKey == null) {
                    startKey = queryKeyRanges.get(j).getLowerRange();
                    startKeyInclusive = queryKeyRanges.get(j).isLowerInclusive();
                }

                if (theCurrentGuidePost == null) {
                    InnerRangeQueryResult innerRangeQueryResult = rangeQuery(lowestCommonAncestor,
                            KeyRange.getKeyRange(startKey, startKeyInclusive, queryKeyRanges.get(j).getUpperRange(), queryKeyRanges.get(j).isUpperInclusive()));
                    assert (innerRangeQueryResult != null);

                    accumulatedEstimation.merge(innerRangeQueryResult.getAccumulatedEstimation());

                    int compareQueryUpperRangeToGuidePost = 1;
                    while (innerRangeQueryResult.getGuidePostIterator().hasNext()) {
                        theCurrentGuidePost = innerRangeQueryResult.getGuidePostIterator().next();
                        compareQueryUpperRangeToGuidePost = queryKeyRanges.get(j).compareUpperRange(
                                theCurrentGuidePost.getGuidePostKey(), 0, theCurrentGuidePost.getGuidePostKey().length, true);
                        if (compareQueryUpperRangeToGuidePost >= 0) {
                            parallelScanRanges.add(KeyRange.getKeyRange(startKey, startKeyInclusive, theCurrentGuidePost.getGuidePostKey(), false));
                            startKey = theCurrentGuidePost.getGuidePostKey();
                            startKeyInclusive = true;
                        } else {
                            // This should be the first guide post cover the query range and the last guide post in the iterator.
                            byte[] endKey = queryKeyRanges.get(j).isUpperInclusive() ?
                                    theCurrentGuidePost.getGuidePostKey() : queryKeyRanges.get(j).getUpperRange();
                            parallelScanRanges.add(KeyRange.getKeyRange(startKey, startKeyInclusive, endKey, false));
                            startKey = null;
                            startKeyInclusive = false;
                        }
                    }

                    if (compareQueryUpperRangeToGuidePost > 0) {
                        // This should only happen when all the remaining query key ranges, including the current one,
                        // exceed the upper bound of the guide posts, i.e., the end key of the last guide post. In this case,
                        // there is no guide post estimation for these ranges, but we need to generate scan range for it.
                        assert(i == (queryKeyRangesGroupedByRegion.size() - 1));
                        break;
                    }
                }
                else {
                    int compareQueryUpperRangeToGuidePost = queryKeyRanges.get(j).compareUpperRange(
                            theCurrentGuidePost.getGuidePostKey(), 0, theCurrentGuidePost.getGuidePostKey().length, true);
                    if (compareQueryUpperRangeToGuidePost < 0) {
                        continue; // The current query key range is within the current guide post. Move forward and check the next query key range.
                    } else if (compareQueryUpperRangeToGuidePost == 0) {
                        parallelScanRanges.add(KeyRange.getKeyRange(startKey, startKeyInclusive, theCurrentGuidePost.getGuidePostKey(), false));
                        startKey = theCurrentGuidePost.getGuidePostKey();
                        startKeyInclusive = true;
                        theCurrentGuidePost = null;
                    } else {
                        // In the case that the current guide post is left to query key range's upper range
                        int compareQueryLowerRangeToGuidePost = queryKeyRanges.get(j).compareLowerToUpperBound(
                                theCurrentGuidePost.getGuidePostKey(), comparator);
                        if (compareQueryLowerRangeToGuidePost < 0) {
                            // In the case that the current guide post is within (query lower range, query upper range) with both ends exclusive
                            parallelScanRanges.add(KeyRange.getKeyRange(startKey, startKeyInclusive, theCurrentGuidePost.getGuidePostKey(), false));
                            startKey = theCurrentGuidePost.getGuidePostKey();
                            startKeyInclusive = true;
                        } else {
                            // In the case that query key lower range is at or right to the current guide post
                            byte[] endKey = queryKeyRanges.get(j - 1).isUpperInclusive() ?
                                    theCurrentGuidePost.getGuidePostKey() : queryKeyRanges.get(j - 1).getUpperRange();
                            parallelScanRanges.add(KeyRange.getKeyRange(startKey, startKeyInclusive, endKey, false));
                            startKey = queryKeyRanges.get(j).getLowerRange();
                            startKeyInclusive = queryKeyRanges.get(j).isLowerInclusive();
                        }

                        theCurrentGuidePost = null;
                        j--; // step back to continue with the current query range but new start key
                    }
                }
            }

            if (startKey != null) {
                byte[] endKey = queryKeyRanges.get(queryKeyRanges.size() - 1).isUpperInclusive() ?
                        regionKeyRanges.get(regionIndex).getUpperRange() : queryKeyRanges.get(queryKeyRanges.size() - 1).getUpperRange();
                parallelScanRanges.add(KeyRange.getKeyRange(startKey, startKeyInclusive, endKey, false));
            }

            parallelScanRangesGroupedByRegion.add(parallelScanRanges);
        }

        return new RangeQueryResult(accumulatedEstimation, parallelScanRangesGroupedByRegion);
    }
}