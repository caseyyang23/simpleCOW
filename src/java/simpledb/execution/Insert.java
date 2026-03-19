package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private final TransactionId tid;
    private OpIterator child;
    private final int tableId;
    private final TupleDesc resultTupleDesc;
    private boolean fetched;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        if (!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId))) {
            throw new DbException("child tupledesc differs from table tupledesc");
        }
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
        this.resultTupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        this.fetched = false;
    }

    public TupleDesc getTupleDesc() {
        return resultTupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        fetched = false;
        super.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        fetched = false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (fetched) {
            return null;
        }

        int count = 0;
        try {
            while (child.hasNext()) {
                Tuple tuple = child.next();
                Database.getBufferPool().insertTuple(tid, tableId, tuple);
                count++;
            }
        } catch (IOException e) {
            throw new DbException("insert failed: " + e.getMessage());
        }

        Tuple result = new Tuple(resultTupleDesc);
        result.setField(0, new IntField(count));
        fetched = true;
        return result;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
    }
}
