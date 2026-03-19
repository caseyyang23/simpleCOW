package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pageSize = BufferPool.getPageSize();
        long offset = (long) pid.getPageNumber() * pageSize;
        byte[] data = new byte[pageSize];

        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            if (offset < 0 || offset + pageSize > raf.length()) {
                throw new IllegalArgumentException("page does not exist");
            }
            raf.seek(offset);
            raf.readFully(data);
            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            throw new IllegalArgumentException("unable to read page", e);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        int pageSize = BufferPool.getPageSize();
        long offset = (long) page.getId().getPageNumber() * pageSize;
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(offset);
            raf.write(page.getPageData());
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> modifiedPages = new ArrayList<>();

        for (int i = 0; i < numPages(); i++) {
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(
                    tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                modifiedPages.add(page);
                return modifiedPages;
            }
        }

        try (BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(file, true))) {
            bw.write(HeapPage.createEmptyPageData());
        }

        HeapPage newPage = new HeapPage(new HeapPageId(getId(), numPages() - 1), HeapPage.createEmptyPageData());
        newPage.insertTuple(t);
        writePage(newPage);
        modifiedPages.add(newPage);
        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        ArrayList<Page> modifiedPages = new ArrayList<>();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(
                tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        modifiedPages.add(page);
        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new AbstractDbFileIterator() {
            private int pageNo;
            private Iterator<Tuple> tupleIterator;
            private boolean open;

            @Override
            public void open() {
                pageNo = 0;
                tupleIterator = null;
                open = true;
            }

            @Override
            protected Tuple readNext() throws DbException, TransactionAbortedException {
                if (!open) {
                    return null;
                }

                while (pageNo < numPages()) {
                    if (tupleIterator == null) {
                        HeapPageId pid = new HeapPageId(getId(), pageNo);
                        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                        tupleIterator = page.iterator();
                    }

                    if (tupleIterator.hasNext()) {
                        return tupleIterator.next();
                    }

                    pageNo++;
                    tupleIterator = null;
                }
                return null;
            }

            @Override
            public void rewind() {
                close();
                open();
            }

            @Override
            public void close() {
                super.close();
                pageNo = 0;
                tupleIterator = null;
                open = false;
            }
        };
    }

}
