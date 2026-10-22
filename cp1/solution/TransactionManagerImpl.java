/*
 * University of Warsaw
 * Concurrent Programming Course 2020/2021
 * Java Assignment
 * 
 * Author: Mykhailo Shevchenko (ms420826@students.mimuw.edu.pl)
 */

package cp1.solution;

import java.lang.Math;

import java.util.Collection;
import java.util.List;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.Set;
import java.util.Map;
import java.util.HashSet;

import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;

import cp1.base.AnotherTransactionActiveException;
import cp1.base.NoActiveTransactionException;
import cp1.base.UnknownResourceIdException;
import cp1.base.ActiveTransactionAborted;
import cp1.base.ResourceOperationException;

import cp1.base.LocalTimeProvider;
import cp1.base.ResourceId;
import cp1.base.Resource;
import cp1.base.ResourceOperation;
import cp1.base.TransactionManager;

public class TransactionManagerImpl implements TransactionManager {

    // Signals that a Resource does not have a thread assigned.
    private final static long NO_THREAD = 0L;
    private LocalTimeProvider timeProvider;

    // A pool of resources managed by TM.
    private Map<ResourceId, Resource> resources;

    // Mutex for graph data (owner, waitingOn)
    private ReentrantLock graphMutex;
    // Conditions for waiting for resource access.
    private ConcurrentMap<ResourceId, Condition> resourceConds;

    // owner[resourceId] = threadId iff thread with threadId
    // operated previously on the resource with resourceId.
    private ConcurrentMap<ResourceId, Long> owner;
    // Inversion of `owner`.
    private ConcurrentMap<Long, List<ResourceId>> owns;
    // waitingOn[x] = y iff thread x wants resource y.
    private ConcurrentMap<Long, ResourceId> waitingOn;

    // There is a key-value pair here iff a thread has an active transaction.
    // True if was aborted, false otherwise.
    private ConcurrentMap<Long, Boolean> aborted;
    private ConcurrentMap<Long, Thread> threads;
    // Transaction history for each thread.
    private ConcurrentMap<Long, Transaction> transactions;

    // Check whether there is a cycle in the graph starting at `startThread`.
    // Return `null` if there is no such cycle, else return the beginning of it.
    // Critical section.
    private Long checkCycle(Long threadId) {
        Set<Long> visited = new HashSet<Long>();
        visited.add(threadId);

        ResourceId resourceId = waitingOn.get(threadId);
        threadId = owner.get(resourceId);

        while (resourceId != null && !visited.contains(threadId)) {
            visited.add(threadId);
            resourceId = waitingOn.get(threadId);
            if (resourceId != null) {
                threadId = owner.get(resourceId);
            }
        }

        return resourceId == null ? null : threadId;
    }

    // Receive the beginning of a cycle.
    // Return a thread responsible for the latest starting transaction.
    // Critical section.
    private Long findLatest(Long cycleStart) {
        Long latest = cycleStart;
        ResourceId resourceId = waitingOn.get(cycleStart);
        assert resourceId != null;
        Long i = owner.get(resourceId);
        long latestTime = transactions.get(latest).getTime();

        while (i != cycleStart) {
            long i_time = transactions.get(i).getTime();
            if (latestTime < i_time || (latestTime == i_time && latest < i)) {
                latestTime = i_time;
                latest = i;
            }

            resourceId = waitingOn.get(i);
            assert resourceId != null;
            i = owner.get(resourceId);
        }

        return latest;
    }

    private void acquireResource(ResourceId rid) throws InterruptedException,
            ActiveTransactionAborted {
        graphMutex.lockInterruptibly();
        Long currentThread = Thread.currentThread().getId();
        if (owner.get(rid) != currentThread) {
            while (owner.get(rid) != NO_THREAD) {
                waitingOn.put(currentThread, rid);
                if (!aborted.get(owner.get(rid))) {
                    Long cycleStart = checkCycle(currentThread);
                    if (cycleStart != null) {
                        Long latest = findLatest(cycleStart);
                        // Abort the thread. Could abort self.
                        waitingOn.remove(latest);
                        aborted.put(latest, true);
                        threads.get(latest).interrupt();
                    }
                }

                try {
                    resourceConds.get(rid).await();
                } catch (InterruptedException e) {
                    graphMutex.unlock();
                    if (isTransactionAborted()) {
                        throw new ActiveTransactionAborted();
                    } else {
                        throw e;
                    }
                }
            }
            owner.put(rid, currentThread);
            owns.compute(currentThread, (_, owned) -> {
                if (owned == null) {
                    owned = new ArrayList<ResourceId>();
                }
                owned.add(rid);
                return owned;
            });
            waitingOn.remove(currentThread);
        }
        graphMutex.unlock();
    }

    public TransactionManagerImpl(
            Collection<Resource> resources,
            LocalTimeProvider timeProvider) {

        this.timeProvider = timeProvider;
        aborted = new ConcurrentHashMap<Long, Boolean>();
        threads = new ConcurrentHashMap<Long, Thread>();
        transactions = new ConcurrentHashMap<Long, Transaction>();
        waitingOn = new ConcurrentHashMap<Long, ResourceId>();
        owns = new ConcurrentHashMap<Long, List<ResourceId>>();
        graphMutex = new ReentrantLock(true);
        this.resources = new ConcurrentHashMap<ResourceId, Resource>();
        owner = new ConcurrentHashMap<ResourceId, Long>();
        resourceConds = new ConcurrentHashMap<ResourceId, Condition>();
        for (Resource r : resources) {
            ResourceId id = r.getId();
            this.resources.put(id, r);
            owner.put(id, NO_THREAD);
            resourceConds.put(id, graphMutex.newCondition());
        }
    }

    public void startTransaction() throws AnotherTransactionActiveException {

        long startTime = timeProvider.getTime();
        if (isTransactionActive()) {
            throw new AnotherTransactionActiveException();
        } else {
            Thread currentThread = Thread.currentThread();
            long id = currentThread.getId();
            aborted.put(id, false);
            threads.put(id, currentThread);
            transactions.put(id, new Transaction(startTime, resources));
        }
    }

    public void operateOnResourceInCurrentTransaction(
            ResourceId rid,
            ResourceOperation operation) throws NoActiveTransactionException,
            UnknownResourceIdException,
            ActiveTransactionAborted,
            ResourceOperationException,
            InterruptedException {

        if (!isTransactionActive())
            throw new NoActiveTransactionException();
        if (resources.get(rid) == null)
            throw new UnknownResourceIdException(rid);
        if (isTransactionAborted())
            throw new ActiveTransactionAborted();
        acquireResource(rid);
        assert !Thread.currentThread().isInterrupted();
        try {
            Resource r = resources.get(rid);
            operation.execute(r);
            if (Thread.currentThread().isInterrupted()) {
                operation.undo(r);
                throw new InterruptedException();
            }
            transactions.get(Thread.currentThread().getId()).operated(operation, rid);
        } catch (ResourceOperationException e) {
            throw e;
        }
    }

    private void finishTransaction() {
        long currentThread = Thread.currentThread().getId();
        graphMutex.lock();
        // Don't need anything.
        waitingOn.remove(currentThread);
        aborted.remove(currentThread);
        threads.remove(currentThread);
        transactions.remove(currentThread);

        // Delete signs of having access to resources.
        List<ResourceId> owned = owns.get(currentThread);
        if (owned != null) {
            for (ResourceId rid : owned) {
                owner.put(rid, NO_THREAD);
                resourceConds.get(rid).signal();
            }
            owned.clear();
        }
        graphMutex.unlock();
    }

    public void commitCurrentTransaction() throws NoActiveTransactionException,
            ActiveTransactionAborted {

        if (!isTransactionActive())
            throw new NoActiveTransactionException();
        if (isTransactionAborted())
            throw new ActiveTransactionAborted();

        finishTransaction();
    }

    public void rollbackCurrentTransaction() {
        long currentThread = Thread.currentThread().getId();
        Transaction t = transactions.get(currentThread);
        // Undo operations.
        if (t != null) {
            t.rollback();
            finishTransaction();
        }
    }

    public boolean isTransactionActive() {
        return aborted.get(Thread.currentThread().getId()) != null;
    }

    public boolean isTransactionAborted() {
        Boolean isAborted = aborted.get(Thread.currentThread().getId());
        return isAborted != null && isAborted;
    }
}
