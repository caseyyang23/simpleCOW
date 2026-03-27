package simpledb.transaction;

import simpledb.common.Permissions;
import simpledb.storage.PageId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LockManager {
    private static final long LOCK_TIMEOUT_MS = 1000;

    private final Map<PageId, List<PageLock>> pageLocks = new HashMap<>();
    private final Map<TransactionId, Set<PageId>> transactionLocks = new HashMap<>();

    public synchronized Boolean acquireLock(TransactionId tid, PageId pageId, Permissions permissions)
            throws TransactionAbortedException {
        long deadline = System.currentTimeMillis() + LOCK_TIMEOUT_MS;

        while (true) {
            List<PageLock> locks = pageLocks.computeIfAbsent(pageId, k -> new ArrayList<>());
            PageLock existing = findLock(tid, locks);

            if (canGrant(tid, permissions, locks, existing)) {
                grantLock(tid, pageId, permissions, locks, existing);
                return true;
            }

            if (existing != null
                    && existing.getPermissions() == Permissions.READ_ONLY
                    && permissions == Permissions.READ_WRITE
                    && locks.size() > 1) {
                throw new TransactionAbortedException();
            }

            long waitTime = deadline - System.currentTimeMillis();
            if (waitTime <= 0) {
                throw new TransactionAbortedException();
            }

            try {
                wait(Math.min(waitTime, 50));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new TransactionAbortedException();
            }
        }
    }

    public synchronized void releaseLock(TransactionId tid, PageId pageId) {
        List<PageLock> locks = pageLocks.get(pageId);
        if (locks != null) {
            locks.removeIf(lock -> lock.getTransactionId().equals(tid));
            if (locks.isEmpty()) {
                pageLocks.remove(pageId);
            }
        }

        Set<PageId> heldPages = transactionLocks.get(tid);
        if (heldPages != null) {
            heldPages.remove(pageId);
            if (heldPages.isEmpty()) {
                transactionLocks.remove(tid);
            }
        }

        notifyAll();
    }

    public synchronized void releaseAllLock(TransactionId transactionId) {
        Set<PageId> heldPages = transactionLocks.get(transactionId);
        if (heldPages == null) {
            return;
        }

        List<PageId> pagesToRelease = new ArrayList<>(heldPages);
        for (PageId pageId : pagesToRelease) {
            releaseLock(transactionId, pageId);
        }
    }

    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        List<PageLock> locks = pageLocks.get(pid);
        return findLock(tid, locks) != null;
    }

    private boolean canGrant(TransactionId tid, Permissions permissions, List<PageLock> locks, PageLock existing) {
        if (locks.isEmpty()) {
            return true;
        }

        if (existing != null) {
            if (existing.getPermissions() == Permissions.READ_WRITE) {
                return true;
            }
            if (permissions == Permissions.READ_ONLY) {
                return true;
            }
            return locks.size() == 1;
        }

        if (permissions == Permissions.READ_ONLY) {
            for (PageLock lock : locks) {
                if (lock.getPermissions() == Permissions.READ_WRITE) {
                    return false;
                }
            }
            return true;
        }

        return false;
    }

    private void grantLock(TransactionId tid, PageId pageId, Permissions permissions, List<PageLock> locks, PageLock existing) {
        if (existing != null) {
            existing.setPermissions(permissions);
        } else {
            locks.add(new PageLock(tid, pageId, permissions));
            transactionLocks.computeIfAbsent(tid, k -> new HashSet<>()).add(pageId);
        }
    }

    private PageLock findLock(TransactionId tid, List<PageLock> locks) {
        if (locks == null) {
            return null;
        }
        for (PageLock lock : locks) {
            if (lock.getTransactionId().equals(tid)) {
                return lock;
            }
        }
        return null;
    }
}
