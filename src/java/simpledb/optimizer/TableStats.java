package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    private final int tableId;
    private final int ioCostPerPage;
    private final DbFile dbFile;
    private final TupleDesc tupleDesc;
    private final Map<Integer, IntHistogram> intHistogramMap;
    private final Map<Integer, StringHistogram> stringHistogramMap;
    private int totalTuples;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        this.tableId = tableid;
        this.ioCostPerPage = ioCostPerPage;
        this.dbFile = Database.getCatalog().getDatabaseFile(tableid);
        this.tupleDesc = dbFile.getTupleDesc();
        this.intHistogramMap = new HashMap<>();
        this.stringHistogramMap = new HashMap<>();
        this.totalTuples = 0;

        int fields = tupleDesc.numFields();
        int[] mins = new int[fields];
        int[] maxs = new int[fields];
        for (int i = 0; i < fields; i++) {
            mins[i] = Integer.MAX_VALUE;
            maxs[i] = Integer.MIN_VALUE;
        }

        Transaction tid = new Transaction();
        try {
            DbFileIterator iterator = dbFile.iterator(tid.getId());
            iterator.open();
            while (iterator.hasNext()) {
                Tuple tuple = iterator.next();
                totalTuples++;
                for (int i = 0; i < fields; i++) {
                    if (tupleDesc.getFieldType(i) == Type.INT_TYPE) {
                        int value = ((IntField) tuple.getField(i)).getValue();
                        mins[i] = Math.min(mins[i], value);
                        maxs[i] = Math.max(maxs[i], value);
                    }
                }
            }
            iterator.close();

            for (int i = 0; i < fields; i++) {
                if (tupleDesc.getFieldType(i) == Type.INT_TYPE) {
                    if (mins[i] == Integer.MAX_VALUE) {
                        mins[i] = 0;
                        maxs[i] = 0;
                    }
                    intHistogramMap.put(i, new IntHistogram(NUM_HIST_BINS, mins[i], maxs[i]));
                } else {
                    stringHistogramMap.put(i, new StringHistogram(NUM_HIST_BINS));
                }
            }

            iterator = dbFile.iterator(tid.getId());
            iterator.open();
            while (iterator.hasNext()) {
                Tuple tuple = iterator.next();
                for (int i = 0; i < fields; i++) {
                    if (tupleDesc.getFieldType(i) == Type.INT_TYPE) {
                        intHistogramMap.get(i).addValue(((IntField) tuple.getField(i)).getValue());
                    } else {
                        stringHistogramMap.get(i).addValue(((StringField) tuple.getField(i)).getValue());
                    }
                }
            }
            iterator.close();
        } catch (DbException | TransactionAbortedException e) {
            throw new RuntimeException("failed to compute table stats", e);
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return ((HeapFile) dbFile).numPages() * (double) ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) (totalTuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        if (tupleDesc.getFieldType(field) == Type.INT_TYPE) {
            return intHistogramMap.get(field).avgSelectivity();
        }
        return stringHistogramMap.get(field).avgSelectivity();
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if (tupleDesc.getFieldType(field).equals(Type.INT_TYPE)) {
            IntField intField = (IntField) constant;
            return intHistogramMap.get(field).estimateSelectivity(op, intField.getValue());
        } else {
            StringField stringField = (StringField) constant;
            return stringHistogramMap.get(field).estimateSelectivity(op, stringField.getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return totalTuples;
    }

}
