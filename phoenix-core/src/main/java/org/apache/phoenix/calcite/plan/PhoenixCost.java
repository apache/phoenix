package org.apache.phoenix.calcite.plan;

import java.util.Objects;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptUtil;

/**
 * Implementation of Calcite {@link org.apache.calcite.plan.RelOptCost}
 * for Phoenix.
 */
public class PhoenixCost implements RelOptCost, Comparable<RelOptCost> {
    //~ Static fields/initializers ---------------------------------------------

    static final PhoenixCost INFINITY =
            new PhoenixCost(
                    Double.POSITIVE_INFINITY,
                    Double.POSITIVE_INFINITY,
                    Double.POSITIVE_INFINITY) {
        public String toString() {
            return "{inf}";
        }
    };

    static final PhoenixCost HUGE =
            new PhoenixCost(
                    Double.MAX_VALUE,
                    Double.MAX_VALUE,
                    Double.MAX_VALUE) {
        public String toString() {
            return "{huge}";
        }
    };

    static final PhoenixCost ZERO =
            new PhoenixCost(0.0, 0.0, 0.0) {
        public String toString() {
            return "{zero}";
        }
    };

    static final PhoenixCost TINY =
            new PhoenixCost(1.0, 1.0, 1.0) {
        public String toString() {
            return "{tiny}";
        }
    };

    public static final RelOptCostFactory FACTORY = new Factory();

    //~ Instance fields --------------------------------------------------------

    final double cpu;
    final double io;
    final double rowCount;

    //~ Constructors -----------------------------------------------------------

    PhoenixCost(double rowCount, double cpu, double io) {
        this.rowCount = rowCount;
        this.cpu = cpu;
        this.io = io;
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public double getRows() {
        return rowCount;
    }

    @Override
    public double getCpu() {
        return cpu;
    }

    @Override
    public double getIo() {
        return io;
    }

    @Override
    public boolean isInfinite() {
        return (this == INFINITY)
                || (this.rowCount == Double.POSITIVE_INFINITY)
                || (this.cpu == Double.POSITIVE_INFINITY)
                || (this.io == Double.POSITIVE_INFINITY);
    }

    @Override
    public boolean isLe(RelOptCost other) {
        return this.compareTo(other) <= 0;
    }

    public boolean isLt(RelOptCost other) {
        return this.compareTo(other) < 0;
    }

    @Override
    public int compareTo(RelOptCost other) {
        if (this == other) {
            return 0;
        }
        
        PhoenixCost that = (PhoenixCost) other;
        if (this.rowCount != that.rowCount) {
            return this.rowCount < that.rowCount ? -1 : 1;
        }
        
        double thisSum = this.cpu + this.io;
        double thatSum = that.cpu + that.io;
        return thisSum == thatSum ? 0 : (thisSum < thatSum ? -1 : 1);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowCount, cpu, io);
    }

    @Override
    public boolean equals(RelOptCost other) {
        return (other instanceof PhoenixCost)
                && this.compareTo(other) == 0;
    }

    public boolean isEqWithEpsilon(RelOptCost other) {
        if (!(other instanceof PhoenixCost)) {
            return false;
        }
        PhoenixCost that = (PhoenixCost) other;
        return (this == that)
                || ((Math.abs(this.rowCount - that.rowCount) < RelOptUtil.EPSILON)
                        && (Math.abs(this.cpu - that.cpu) < RelOptUtil.EPSILON)
                        && (Math.abs(this.io - that.io) < RelOptUtil.EPSILON));
    }

    public RelOptCost minus(RelOptCost other) {
        if (this == INFINITY) {
            return this;
        }
        PhoenixCost that = (PhoenixCost) other;
        return new PhoenixCost(
                this.rowCount - that.rowCount,
                this.cpu - that.cpu,
                this.io - that.io);
    }

    public RelOptCost multiplyBy(double factor) {
        if (this == INFINITY) {
            return this;
        }
        return new PhoenixCost(rowCount * factor, cpu * factor, io * factor);
    }

    public double divideBy(RelOptCost cost) {
        // Compute the geometric average of the ratios of all of the factors
        // which are non-zero and finite.
        PhoenixCost that = (PhoenixCost) cost;
        double d = 1;
        double n = 0;
        if ((this.rowCount != 0)
                && !Double.isInfinite(this.rowCount)
                && (that.rowCount != 0)
                && !Double.isInfinite(that.rowCount)) {
            d *= this.rowCount / that.rowCount;
            ++n;
        }
        if ((this.cpu != 0)
                && !Double.isInfinite(this.cpu)
                && (that.cpu != 0)
                && !Double.isInfinite(that.cpu)) {
            d *= this.cpu / that.cpu;
            ++n;
        }
        if ((this.io != 0)
                && !Double.isInfinite(this.io)
                && (that.io != 0)
                && !Double.isInfinite(that.io)) {
            d *= this.io / that.io;
            ++n;
        }
        if (n == 0) {
            return 1.0;
        }
        return Math.pow(d, 1 / n);
    }

    public RelOptCost plus(RelOptCost other) {
        PhoenixCost that = (PhoenixCost) other;
        if ((this == INFINITY) || (that == INFINITY)) {
            return INFINITY;
        }
        return new PhoenixCost(
                this.rowCount + that.rowCount,
                this.cpu + that.cpu,
                this.io + that.io);
    }

    public String toString() {
        return "{" + rowCount + " rows, " + cpu + " cpu, " + io + " io}";
    }

    /** Implementation of {@link org.apache.calcite.plan.RelOptCostFactory}
     * that creates {@link org.apache.phoenix.calcite.plan.PhoenixCost}s. */
    private static class Factory implements RelOptCostFactory {
      public RelOptCost makeCost(double dRows, double dCpu, double dIo) {
        return new PhoenixCost(dRows, dCpu, dIo);
      }

      public RelOptCost makeHugeCost() {
        return PhoenixCost.HUGE;
      }

      public RelOptCost makeInfiniteCost() {
        return PhoenixCost.INFINITY;
      }

      public RelOptCost makeTinyCost() {
        return PhoenixCost.TINY;
      }

      public RelOptCost makeZeroCost() {
        return PhoenixCost.ZERO;
      }
    }
}
