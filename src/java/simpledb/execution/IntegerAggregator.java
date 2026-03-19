package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private final Map<Field, Integer> aggregateValues;
    private final Map<Field, Integer> counts;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.aggregateValues = new LinkedHashMap<>();
        this.counts = new LinkedHashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupField = gbfield == NO_GROUPING ? null : tup.getField(gbfield);
        int value = ((IntField) tup.getField(afield)).getValue();

        if (!aggregateValues.containsKey(groupField)) {
            switch (what) {
                case MIN:
                case MAX:
                case SUM:
                case AVG:
                    aggregateValues.put(groupField, value);
                    break;
                case COUNT:
                    aggregateValues.put(groupField, 1);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported aggregation op: " + what);
            }
            counts.put(groupField, 1);
            return;
        }

        int current = aggregateValues.get(groupField);
        int currentCount = counts.get(groupField);
        switch (what) {
            case MIN:
                aggregateValues.put(groupField, Math.min(current, value));
                break;
            case MAX:
                aggregateValues.put(groupField, Math.max(current, value));
                break;
            case SUM:
                aggregateValues.put(groupField, current + value);
                break;
            case COUNT:
                aggregateValues.put(groupField, current + 1);
                break;
            case AVG:
                aggregateValues.put(groupField, current + value);
                break;
            default:
                throw new IllegalArgumentException("Unsupported aggregation op: " + what);
        }
        counts.put(groupField, currentCount + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        ArrayList<Tuple> tuples = new ArrayList<>();
        TupleDesc td;

        if (gbfield == NO_GROUPING) {
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
            Integer aggregateValue = aggregateValues.get(null);
            if (aggregateValue != null) {
                if (what == Op.AVG) {
                    aggregateValue = aggregateValue / counts.get(null);
                }
                Tuple tuple = new Tuple(td);
                tuple.setField(0, new IntField(aggregateValue));
                tuples.add(tuple);
            }
        } else {
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
            for (Map.Entry<Field, Integer> entry : aggregateValues.entrySet()) {
                int aggregateValue = entry.getValue();
                if (what == Op.AVG) {
                    aggregateValue = aggregateValue / counts.get(entry.getKey());
                }
                Tuple tuple = new Tuple(td);
                tuple.setField(0, entry.getKey());
                tuple.setField(1, new IntField(aggregateValue));
                tuples.add(tuple);
            }
        }

        return new TupleIterator(td, tuples);
    }

}
