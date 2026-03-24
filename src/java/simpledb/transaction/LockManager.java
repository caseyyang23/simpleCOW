package simpledb.transaction;

import simpledb.common.DeadlockException;
import simpledb.common.Permissions;
import simpledb.storage.PageId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LockManager {
    private static final long DEADLOCK_TIMEOUT_MS = 1000;

    private final Map<PageId, PageLock> pageLocks = new HashMap<>();
    private final Map<TransactionId, Set<PageId>> transactionLocks = new HashMap<>();

    public synchronized Boolean acquireLock(TransactionId tid, PageId pageId, Permissions permissions)
            throws DeadlockException {
        long startTime = System.currentTimeMillis();
        while (true) {
            PageLock lock = pageLocks.get(pageId);
            if (lock == null) {
                lock = new PageLock(pageId, permissions);
                lock.getHolders().add(tid);
                pageLocks.put(pageId, lock);
                addTransactionLock(tid, pageId);
                return true;
            }

            boolean holdsLock = lock.getHolders().contains(tid);
            if (holdsLock) {
                if (permissions == Permissions.READ_ONLY) {
                    return true;
                }
                if (lock.getPermissions() == Permissions.READ_WRITE) {
                    return true;
                }
                if (lock.getHolders().size() == 1) {
                    lock.setPermissions(Permissions.READ_WRITE);
                    return true;
                }
                throw new DeadlockException();
            }

            if (permissions == Permissions.READ_ONLY) {
                if (lock.getPermissions() == Permissions.READ_ONLY) {
                    lock.getHolders().add(tid);
                    addTransactionLock(tid, pageId);
                    return true;
                }
                waitForLock(startTime);
                continue;
            }

            waitForLock(startTime);
        }
    }

    public synchronized void releaseLock(TransactionId tid, PageId pageId) {
        PageLock lock = pageLocks.get(pageId);
        if (lock == null) {
            return;
        }

        if (lock.getHolders().remove(tid)) {
            removeTransactionLock(tid, pageId);
            if (lock.getHolders().isEmpty()) {
                pageLocks.remove(pageId);
            }
            notifyAll();
        }
    }

    public synchronized void releaseAllLock(TransactionId transactionId) {
        Set<PageId> lockedPages = transactionLocks.get(transactionId);
        if (lockedPages == null || lockedPages.isEmpty()) {
            return;
        }
        Set<PageId> pagesToRelease = new HashSet<>(lockedPages);
        for (PageId pageId : pagesToRelease) {
            releaseLock(transactionId, pageId);
        }
    }

    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        PageLock lock = pageLocks.get(pid);
        return lock != null && lock.getHolders().contains(tid);
    }

    private void addTransactionLock(TransactionId tid, PageId pageId) {
        transactionLocks.computeIfAbsent(tid, key -> new HashSet<>()).add(pageId);
    }

    private void removeTransactionLock(TransactionId tid, PageId pageId) {
        Set<PageId> lockedPages = transactionLocks.get(tid);
        if (lockedPages == null) {
            return;
        }
        lockedPages.remove(pageId);
        if (lockedPages.isEmpty()) {
            transactionLocks.remove(tid);
        }
    }

    private void waitForLock(long startTime) throws DeadlockException {
        long elapsed = System.currentTimeMillis() - startTime;
        long remaining = DEADLOCK_TIMEOUT_MS - elapsed;
        if (remaining <= 0) {
            throw new DeadlockException();
        }
        try {
            wait(remaining);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DeadlockException();
        }
    }
}
