package simpledb.transaction;

import simpledb.common.Permissions;
import simpledb.storage.PageId;

import java.util.HashSet;
import java.util.Set;

public class PageLock {
    private PageId pageId;
    private Permissions permissions;
    private Set<TransactionId> holders;

    public PageLock(PageId pageId, Permissions permissions) {
        this.pageId = pageId;
        this.permissions = permissions;
        this.holders = new HashSet<>();
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

    public Set<TransactionId> getHolders() {
        return holders;
    }

    public void setHolders(Set<TransactionId> holders) {
        this.holders = holders;
    }
}
