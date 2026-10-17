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
import java.util.Set;
import java.util.Map;
import java.util.HashMap;

import java.util.concurrent.Semaphore;
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

public class Manager implements TransactionManager {
    /* In this solution I'm using thread's ids to differentiate threads.
    This solution has a drawback s. t. if thread A stops having an active
    transaction and thread B is created with A's id, it inherits A's transaction.
    However I believe this is acceptable by a grader.
    */

    // Signals that Resource does not have a thread assigned.
    private final static long NO_THREAD = 0L;
    private LocalTimeProvider timeProvider;

    // A pool of resources that Manager manages.
    private Map<ResourceId, Resource> resources;

    // access[resourceId] = threadId iff thread with threadId
    // operated previously on the resource with resourceId.
    private ConcurrentMap<ResourceId, Long> access;

    // If a thread wants a reasource it needs to acquire it.
    private ConcurrentMap<ResourceId, Semaphore> resource_safety;

    // graph[x] = y, x wants resource y.
    private ConcurrentMap<Long, ResourceId> graph;
    // Mutex for graph data (access, graph)
    private Semaphore graph_mutex;

    // There is key-value here iff a thread has an active transaction.
    // True if was aborted, false otherwise.
    private ConcurrentMap<Long, Boolean> aborted;

    private ConcurrentMap<Long, Thread> threads;

    // Stores transaction history for each thread.
    private ConcurrentMap<Long, Transaction> transactions;

    // Checks whether there is a cycle in the graph starting at v.
    // Returns null if there is no such cycle, else returns the beginning of it.
    private Long check_cycle(Long v) {
        Map<Long, Boolean> visited = new HashMap<Long, Boolean>();
        visited.put(v, true);

        ResourceId id = graph.get(v);
        Long i = access.get(id);

        while (id != null && visited.get(i) == null) {
            visited.put(i, true);
            id = graph.get(i);
            if (id != null) {
                i = access.get(id);
            }
        }

        return id == null ? null : i;
    }

    // Recieves the beginning of a cycle.
    // Returns a thread responsible for the youngest transaction.
    private Long find_min(Long v) {
        Long minimum = v;
        ResourceId id = graph.get(v);
        assert id != null;
        Long i = access.get(id);
        long oldest_time = transactions.get(minimum).get_time();

        while (i != v) {
            long i_time = transactions.get(i).get_time();
            if (oldest_time < i_time || (oldest_time == i_time && minimum < i)) {
                oldest_time = i_time;
                minimum = i;
            }

            i = access.get(graph.get(i));
        }

        return minimum;
    }

    public Manager(
	Collection<Resource> resources,
	LocalTimeProvider timeProvider
    ) {

        this.resources = new ConcurrentHashMap<ResourceId, Resource>();
        for (Resource r : resources)
            this.resources.put(r.getId(), r);
        Set<ResourceId> keySet = this.resources.keySet();

        this.timeProvider = timeProvider;
        access = new ConcurrentHashMap<ResourceId, Long>();
        for (ResourceId r : keySet) {
            access.put(r, NO_THREAD);
        }

        resource_safety = new ConcurrentHashMap<ResourceId, Semaphore>();
        for (ResourceId r : keySet) {
            resource_safety.put(r, new Semaphore(1, true));
        }

        aborted = new ConcurrentHashMap<Long, Boolean>();
        threads = new ConcurrentHashMap<Long, Thread>();
        transactions = new ConcurrentHashMap<Long, Transaction>();
        graph = new ConcurrentHashMap<Long, ResourceId>();
        graph_mutex = new Semaphore(1, true);
    }

    public void startTransaction(
    ) throws
        AnotherTransactionActiveException {

        long start_time = timeProvider.getTime();
        if (isTransactionActive()) {
            throw new AnotherTransactionActiveException();
        } else {
            long curThread = Thread.currentThread().getId();
            aborted.put(curThread, false);
            threads.put(curThread, Thread.currentThread());
            transactions.put(curThread, new Transaction(start_time, resources));
        }
    }

    public void operateOnResourceInCurrentTransaction(
	ResourceId rid,
	ResourceOperation operation
    ) throws
	NoActiveTransactionException,
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

        Long curThread = Thread.currentThread().getId();

        // Need to check if me staying in a queue would cause a cycle.
        graph_mutex.acquire();
        Long owner = access.get(rid);
        if (curThread != owner)
            graph.put(curThread, rid);
        // If I would need to wait (resource is owned by someone else who is not aborted),
        // I need to check for a cycle.
        // If the owner is aborted it is equivalent to the resource being free
        // yet temporarily unavailable.
        // The cycle could happen only if I want something that is not yet free.
        if (owner != curThread && owner != NO_THREAD && !aborted.get(owner)) {

            Long beginning = check_cycle(curThread);
            if (beginning != null) {
                Long minimum = find_min(beginning);
                // Aborted thread doesn't need anything anymore.
                graph.remove(minimum);
                aborted.put(minimum, true);
                threads.get(minimum).interrupt();
            }
        }
        graph_mutex.release();

        boolean tmp = false;
        // After I'm sure there is no cycle I can patiently wait for the resource.
        try {
            if (owner != curThread) {
                resource_safety.get(rid).acquire();
                tmp = true;
            }
            graph_mutex.acquire();
            access.put(rid, curThread);
            // I don't need anybody now.
            graph.remove(curThread);
            graph_mutex.release();

            try {
                operation.execute(resources.get(rid));
                transactions.get(curThread).operated(operation, rid);
            } catch (ResourceOperationException e) {
                throw e;
            }
        } catch (InterruptedException e) {
            // Need to delete dependency from graph.
            if (owner != curThread) {
                if (tmp)
                    resource_safety.get(rid).release();
                remove_from_graph();
            }
            if (isTransactionAborted()) {
                throw new ActiveTransactionAborted();
            } else {
                throw e;
            }
        }
    }

    private void remove_from_graph() {
        try {
            graph_mutex.acquire();
            graph.remove(Thread.currentThread().getId());
            graph_mutex.release();
        } catch (InterruptedException e) {
            remove_from_graph();
        }
    }

    // Tries until it succeedes.
    private void finishTransaction() {
        long curThread = Thread.currentThread().getId();
        try {
            graph_mutex.acquire();
            // Don't need anything.
            graph.remove(curThread);
            aborted.remove(curThread);
            threads.remove(curThread);
            transactions.remove(curThread);

            // Delete signs of having access to resources.
            for (Map.Entry<ResourceId, Long> entry : access.entrySet()) {
                if (curThread == entry.getValue()) {
                    ResourceId id = entry.getKey();
                    access.put(id, NO_THREAD);
                    resource_safety.get(id).release();
                }
            }
            graph_mutex.release();
        } catch (InterruptedException e) {
            finishTransaction();
        }
    }

    public void commitCurrentTransaction(
    ) throws
	NoActiveTransactionException,
	ActiveTransactionAborted {

        if (!isTransactionActive())
            throw new NoActiveTransactionException();
        if (isTransactionAborted())
            throw new ActiveTransactionAborted();

        finishTransaction();
    }

    public void rollbackCurrentTransaction() {
        long curThread = Thread.currentThread().getId();
        Transaction t = transactions.get(curThread);
        // Undo operations.
        if (t != null) {
            t.rollback();
        }

        finishTransaction();
    }

    public boolean isTransactionActive() {
        return aborted.get(Thread.currentThread().getId()) != null;
    }

    public boolean isTransactionAborted() {
        // Has transaction and it was aborted (== true)
        Boolean tmp = aborted.get(Thread.currentThread().getId());
        return tmp != null & tmp == true;
    }

}
