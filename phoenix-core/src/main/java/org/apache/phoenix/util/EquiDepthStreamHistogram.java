/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
/**
 * Equi-Depth histogram based on http://web.cs.ucla.edu/~zaniolo/papers/Histogram-EDBT2011-CamReady.pdf,
 * but without the sliding window - we assume a single window over the entire data set.
 *
 * Used to generate the bucket boundaries of a histogram where each bucket has the same # of items.
 * This is useful, for example, for pre-splitting an index table, by feeding in data from the indexed column.
 * Works on streaming data - the histogram is dynamically updated for each new value.
 *
 * Add values by calling addValue(), then at the end computeBuckets() can be called to get
 * the buckets with their bounds.
 *
 * Average time complexity: O(log(B x p) + (B x p)/T) = nearly constant
 * B = number of buckets, p = expansion factor constant, T = # of values
 *
 * Space complexity: different from paper since here we keep the blocked bars but don't have expiration,
 *  comes out to basically O(log(T))
 */
public class EquiDepthStreamHistogram {
    private static final Logger LOGGER = LoggerFactory.getLogger(EquiDepthStreamHistogram.class);

    // used in maxSize calculation for each bar
    private static final double MAX_COEF = 1.7;
    // higher expansion factor = better accuracy and worse performance
    private static final short DEFAULT_EXPANSION_FACTOR = 7;
    private int numBuckets;
    private int maxBars;
    @VisibleForTesting
    long totalCount; // number of values - i.e. count across all bars
    @VisibleForTesting
    List<Bar> bars;

    /**
     * Create a new histogram
     * @param numBuckets number of buckets, which can be used to get the splits
     */
    public EquiDepthStreamHistogram(int numBuckets) {
        this(numBuckets, DEFAULT_EXPANSION_FACTOR);
    }

    /**
     * @param numBuckets number of buckets
     * @param expansionFactor number of bars = expansionFactor * numBuckets
     * The more bars, the better the accuracy, at the cost of worse performance
     */
    public EquiDepthStreamHistogram(int numBuckets, int expansionFactor) {
        this.numBuckets = numBuckets;
        this.maxBars = numBuckets * expansionFactor;
        this.bars = new ArrayList<>(maxBars);
    }

    /**
     * Add a new value to the histogram, updating the count for the appropriate bucket
     * @param value
     */
    public void addValue(byte[] value) {
        Bar bar = getBar(value);
        bar.incrementCount();
        totalCount++;
        // split the bar if necessary
        if (bar.getSize() > getMaxBarSize()) {
            splitBar(bar);
        }
    }

    /**
     * Compute the buckets, which have the boundaries and estimated counts.
     * Note that the right bound for the very last bucket is inclusive.
     * The left and right bounds can be equivalent, for single value buckets.
     * @return
     */
    public List<Bucket> computeBuckets() {
        Preconditions.checkState(bars.size() >= numBuckets, "Not enough data points to compute buckets");
        List<Bucket> buckets = new ArrayList<>();
        long idealBuckSize = (long) Math.ceil(totalCount / (double) numBuckets);
        long currCount = 0;
        int barsIdx = 0;
        byte[] prevBound = bars.get(0).leftBoundInclusive;
        Bar currBar = null;
        for (int i = 0; i < numBuckets; i++) {
            while (currCount <= idealBuckSize && barsIdx < bars.size()) {
                currBar = bars.get(barsIdx++);
                currCount += currBar.getSize();
            }
            long surplus = Math.max(currCount - idealBuckSize, 0);
            // deviate a bit from the paper here
            // to estimate the bound, we split the range into 8 splits for a total of 10 including start/end
            // then we calculate the % of the currBar's count we've used, and round down to the closest split
            int closestSplitIdx = (int) ((1 - ((double) surplus / currBar.getSize())) * 9);
            byte[][] splits = Bytes.split(currBar.leftBoundInclusive, currBar.rightBoundExclusive, 8);
            Bucket bucket = new Bucket(prevBound, splits[closestSplitIdx]);
            bucket.incrementCountEstimate(currCount - surplus);
            prevBound = splits[closestSplitIdx];
            buckets.add(bucket);
            currCount = surplus;
        }
        return buckets;
    }

    /**
     * @return total number of values added to this histogram
     */
    public long getTotalCount() {
        return totalCount;
    }

    // attempts to split the given bar into two new bars
    @VisibleForTesting
    void splitBar(Bar origBar) {
        // short circuit - don't split a bar of length 1
        if (Bytes.compareTo(origBar.leftBoundInclusive, origBar.rightBoundExclusive) == 0) {
            return;
        }
        if (bars.size() == maxBars) { // max bars hit, need to merge two existing bars first
            boolean mergeSuccessful = mergeBars();
            if (!mergeSuccessful) return; // don't split if we couldn't merge
        }
        byte[] mid = Bytes.split(origBar.getLeftBoundInclusive(), origBar.getRightBoundExclusive(), 1)[1];
        Bar newLeft = new Bar(origBar.getLeftBoundInclusive(), mid);
        Bar newRight = new Bar(mid, origBar.getRightBoundExclusive());
        // distribute blocked bars between the new bars
        long leftSize = 0;
        long bbAggCount = origBar.getBlockedBarsSize();
        for (Bar bb : origBar.getBlockedBars()) {
            long bbSize = bb.getSize();
            if (leftSize + bbSize < bbAggCount/2) {
                leftSize += bbSize;
                newLeft.addBlockedBar(bb);
            } else {
                newRight.addBlockedBar(bb);
            }
        }
        // at this point the two new bars may have different counts,
        // distribute the rest of origBar's count to make them as close as possible
        long countToDistribute = origBar.getSize() - bbAggCount;
        long rightSize = newRight.getSize();
        long sizeDiff = Math.abs(leftSize - rightSize);
        Bar smallerBar = leftSize <= rightSize ? newLeft : newRight;
        if (sizeDiff <= countToDistribute) {
            smallerBar.incrementCount(sizeDiff);
            countToDistribute -= sizeDiff;
            long halfDistrib = countToDistribute / 2;
            newLeft.incrementCount(halfDistrib);
            newRight.incrementCount(countToDistribute - halfDistrib);
        } else {
            smallerBar.incrementCount(countToDistribute);
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format("Split orig=%s , newLeft=%s , newRight=%s",
                    origBar, newLeft, newRight));
        }
        bars.remove(origBar);
        bars.add(newLeft);
        bars.add(newRight);
        // technically don't need to sort here, as we can get the index from getBar,
        // and put the new bars in the same index.  But we'd have to handle merge as well,
        // doable but not worth the more complicated code since bars.size is fixed and generally small
        Collections.sort(bars);
    }

    //Merges the two adjacent bars with the lowest summed count
    @VisibleForTesting
    boolean mergeBars() {
        Preconditions.checkState(bars.size() > 1, "Need at least two bars to merge");
        // pairwise search for the two bars with the smallest summed count
        int currIdx = 0;
        Bar currBar = bars.get(currIdx);
        Bar nextBar = bars.get(currIdx + 1);
        long currMinSum = Long.MAX_VALUE;
        int currMinIdx = currIdx; // keep this for fast removal from ArrayList later
        Pair<Bar, Bar> minBars = new Pair<>(currBar, nextBar);
        while (nextBar != null) {
            long sum = currBar.getSize() + nextBar.getSize();
            if (sum < currMinSum) {
                currMinSum = sum;
                minBars = new Pair<>(currBar, nextBar);
                currMinIdx = currIdx;
            }
            currBar = nextBar;
            nextBar = ++currIdx < bars.size() - 1 ? bars.get(currIdx+1) : null;
        }
        // don't want to merge bars into one that will just need an immediate split again
        if (currMinSum >= getMaxBarSize()) {
            return false;
        }
        // do the merge
        Bar leftBar = minBars.getFirst();
        Bar rightBar = minBars.getSecond();
        Bar newBar = new Bar(leftBar.getLeftBoundInclusive(), rightBar.getRightBoundExclusive());
        if (leftBar.getSize() >= rightBar.getSize()) {
            newBar.incrementCount(rightBar.getCount()); // count of rightBar without its blocked bars
            // this just adds the leftBar without its blocked bars, as we don't want nested blocked bars
            // the leftBar's blocked bars are added later below
            newBar.addBlockedBar(new Bar(leftBar));
        } else {
            newBar.incrementCount(leftBar.getCount());
            newBar.addBlockedBar(new Bar(rightBar));
        }
        newBar.addBlockedBars(leftBar.getBlockedBars());
        newBar.addBlockedBars(rightBar.getBlockedBars());
        bars.subList(currMinIdx, currMinIdx + 2).clear(); // remove minBars
        bars.add(newBar);
        Collections.sort(bars);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format("Merged left=%s , right=%s , newBar=%s", leftBar, rightBar, newBar));
        }
        return true;
    }

    /**
     * Get the appropriate bar for the value, extending existing bar bounds to accommodate if necessary
     * @param value value to add
     * @return the bar for the value
     */
    @VisibleForTesting
    Bar getBar(byte[] value) {
        Bar searchKey = new Bar(value, value);
        int searchIdx = Collections.binarySearch(this.bars, searchKey);
        if (searchIdx < 0) {
            // copy value so later changes by caller don't affect histogram results
            byte[] newBound = Bytes.copy(value);
            if (this.bars.size() == 0) {
                Bar firstBar = new Bar(newBound, newBound);
                bars.add(firstBar);
                return firstBar;
            }
            int expectedIndex = Math.abs(searchIdx + 1); // jdk binary search index
            if (expectedIndex == bars.size()) { // no bars >= value, need to extend rightBound of last bar
                Bar lastBar = bars.get(expectedIndex - 1);
                lastBar.setRightBoundExclusive(newBound); // actually inclusive for last bar
                return lastBar;
            } else { // extend leftBound of next greatest bar
                Bar nextBar = bars.get(expectedIndex);
                nextBar.setLeftBoundInclusive(newBound);
                return nextBar;
            }
        } else {
            return bars.get(searchIdx);
        }
    }

    private long getMaxBarSize() {
        // from the paper,  1.7 has been "determined empirically"
        // interpretation:  We don't want a given bar to deviate more than 70% from its ideal target size
        return (long) (MAX_COEF * (totalCount / maxBars));
    }

    public static class Bucket {
        protected long count = 0;
        protected byte[] leftBoundInclusive;
        protected byte[] rightBoundExclusive;

        public Bucket(byte[] leftBoundInclusive, byte[] rightBoundExclusive) {
            this.leftBoundInclusive = leftBoundInclusive;
            this.rightBoundExclusive = rightBoundExclusive;
        }

        public byte[] getLeftBoundInclusive() {
            return leftBoundInclusive;
        }

        public void setLeftBoundInclusive(byte[] leftBoundInclusive) {
            this.leftBoundInclusive = leftBoundInclusive;
        }

        public byte[] getRightBoundExclusive() {
            return rightBoundExclusive;
        }

        public void setRightBoundExclusive(byte[] rightBoundExclusive) {
            this.rightBoundExclusive = rightBoundExclusive;
        }

        public long getCountEstimate() {
            return count;
        }

        public void incrementCountEstimate(long count) {
            this.count += count;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + Arrays.hashCode(leftBoundInclusive);
            result = prime * result + Arrays.hashCode(rightBoundExclusive);
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            Bucket other = (Bucket) obj;
            if (!Arrays.equals(leftBoundInclusive, other.leftBoundInclusive)) return false;
            if (!Arrays.equals(rightBoundExclusive, other.rightBoundExclusive)) return false;
            return true;
        }

        @Override
        public String toString() {
            return "Bucket [count=" + count + ", leftBoundInclusive="
                    + Bytes.toString(leftBoundInclusive) + ", rightBoundExclusive="
                    + Bytes.toString(rightBoundExclusive) + "]";
        }
    }

    // Used internally to further subdivide each bucket
    @VisibleForTesting
    static class Bar extends Bucket implements Comparable<Bar> {
        private List<Bar> blockedBars = new ArrayList<>(); // populated through a merge

        /**
         * Create a new bar.  Single value buckets can have leftBound = rightBound
         * @param leftBoundInclusive
         * @param rightBoundExclusive
         */
        public Bar(byte[] leftBoundInclusive, byte[] rightBoundExclusive) {
            super(leftBoundInclusive, rightBoundExclusive);
        }

        /**
         * Creates a copy of the passed in bar, but without any blocked bars
         * @param bar
         */
        public Bar(Bar bar) {
            super(bar.leftBoundInclusive, bar.rightBoundExclusive);
            this.count = bar.count;
        }

        // Used to keep the bars sorted by bounds
        @Override
        public int compareTo(Bar other) {
            // if one bar fully contains the other, they are considered the same.  For binary search
            int leftComp = Bytes.compareTo(this.leftBoundInclusive, other.leftBoundInclusive);
            int rightComp = Bytes.compareTo(this.rightBoundExclusive, other.rightBoundExclusive);
            if ((leftComp >= 0 && rightComp < 0) || (leftComp <= 0 && rightComp > 0)
                    || (leftComp == 0 && rightComp == 0)) {
                return 0;
            }
            if (Bytes.compareTo(this.leftBoundInclusive, other.rightBoundExclusive) >= 0) {
                return 1;
            }
            if (Bytes.compareTo(this.rightBoundExclusive, other.leftBoundInclusive) <= 0) {
                return -1;
            }
            throw new AssertionError("Cannot not have overlapping bars");
        }

        /**
         * @return The aggregate count of this bar and its blocked bars' counts
         */
        public long getSize() {
            long blockedBarSum = getBlockedBarsSize();
            return count + blockedBarSum;
        }

        /**
         * @return The sum of the counts of all the blocked bars
         */
        public long getBlockedBarsSize() {
            long blockedBarSum = 0;
            for (Bar bb : blockedBars) {
                blockedBarSum += bb.getSize();
            }
            return blockedBarSum;
        }

        public void addBlockedBar(Bar bar) {
            blockedBars.add(bar);
        }

        public void addBlockedBars(List<Bar> bars) {
            blockedBars.addAll(bars);
        }

        public List<Bar> getBlockedBars() {
            return blockedBars;
        }

        public long getCount() {
            return this.count;
        }

        public void incrementCount() {
            count++;
        }

        public void incrementCount(long increment) {
            count += increment;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = super.hashCode();
            result = prime * result + ((blockedBars == null) ? 0 : blockedBars.hashCode());
            result = prime * result + (int) (count ^ (count >>> 32));
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (!super.equals(obj)) return false;
            if (getClass() != obj.getClass()) return false;
            Bar other = (Bar) obj;
            if (blockedBars == null) {
                if (other.blockedBars != null) return false;
            } else if (!blockedBars.equals(other.blockedBars)) return false;
            if (count != other.count) return false;
            return true;
        }

        @Override
        public String toString() {
            return "Bar[count=" + count + ", blockedBars=" + blockedBars + ", leftBoundInclusive="
                    + Bytes.toString(leftBoundInclusive) + ", rightBoundExclusive="
                    + Bytes.toString(rightBoundExclusive) + "]";
        }
    }
}
