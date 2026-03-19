package simpledb.optimizer;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private final int[] histogram;
    private final int min;
    private final int max;
    private final int buckets;
    private final int width;
    private int ntups;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.histogram = new int[buckets];
        this.width = (int) Math.ceil((max - min + 1) / (double) buckets);
        this.ntups = 0;
    }

    private int bucketIndex(int v) {
        int index = (v - min) / width;
        return Math.min(index, buckets - 1);
    }

    private int bucketLeft(int index) {
        return min + index * width;
    }

    private int bucketRight(int index) {
        return Math.min(max, bucketLeft(index) + width - 1);
    }

    private int bucketWidth(int index) {
        return bucketRight(index) - bucketLeft(index) + 1;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        if (v < min || v > max) {
            return;
        }
        histogram[bucketIndex(v)]++;
        ntups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        if (ntups == 0) {
            return 0.0;
        }

        switch (op) {
            case EQUALS:
                if (v < min || v > max) {
                    return 0.0;
                }
                int eqIndex = bucketIndex(v);
                return (histogram[eqIndex] / (double) bucketWidth(eqIndex)) / ntups;
            case NOT_EQUALS:
                return 1.0 - estimateSelectivity(Predicate.Op.EQUALS, v);
            case GREATER_THAN:
                if (v < min) {
                    return 1.0;
                }
                if (v >= max) {
                    return 0.0;
                }
                int gtIndex = bucketIndex(v);
                double selectivity = 0.0;
                int right = bucketRight(gtIndex);
                selectivity += (histogram[gtIndex] / (double) ntups)
                        * ((right - v) / (double) bucketWidth(gtIndex));
                for (int i = gtIndex + 1; i < buckets; i++) {
                    selectivity += histogram[i] / (double) ntups;
                }
                return selectivity;
            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.GREATER_THAN, v)
                        + estimateSelectivity(Predicate.Op.EQUALS, v);
            case LESS_THAN:
                return 1.0 - estimateSelectivity(Predicate.Op.GREATER_THAN_OR_EQ, v);
            case LESS_THAN_OR_EQ:
                return 1.0 - estimateSelectivity(Predicate.Op.GREATER_THAN, v);
            case LIKE:
                return estimateSelectivity(Predicate.Op.EQUALS, v);
            default:
                return 0.0;
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        if (ntups == 0) {
            return 0.0;
        }
        double total = 0.0;
        for (int i = 0; i < buckets; i++) {
            total += histogram[i] / (double) ntups;
        }
        return total / buckets;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("IntHistogram[");
        for (int i = 0; i < buckets; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(bucketLeft(i)).append("-").append(bucketRight(i)).append(":").append(histogram[i]);
        }
        builder.append("]");
        return builder.toString();
    }
}
