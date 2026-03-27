package simpledb.transaction;

import simpledb.common.Permissions;
import simpledb.storage.PageId;

public class PageLock {
    private TransactionId transactionId;
    private PageId pageId;
    private Permissions permissions;

    public PageLock(TransactionId transactionId, PageId pageId, Permissions permissions) {
        this.transactionId = transactionId;
        this.pageId = pageId;
        this.permissions = permissions;
    }

    public TransactionId getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(TransactionId transactionId) {
        this.transactionId = transactionId;
    }

    public PageId getPageId() {
        return pageId;
    }

    public void setPageId(PageId pageId) {
        this.pageId = pageId;
    }

    public Permissions getPermissions() {
        return permissions;
    }

    public void setPermissions(Permissions permissions) {
        this.permissions = permissions;
    }
}
