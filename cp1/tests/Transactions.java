/*
 * University of Warsaw
 * Concurrent Programming Course 2020/2021
 * Java Assignment
 * 
 * Author: Mykhailo Shevchenko (ms420826@students.mimuw.edu.pl)
 */
package cp1.tests;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import cp1.base.ResourceId;
import cp1.base.ResourceOperation;
import cp1.base.ResourceOperationException;
import cp1.base.TransactionManager;
import cp1.base.UnknownResourceIdException;
import cp1.solution.TransactionManagerFactory;
import cp1.base.ActiveTransactionAborted;
import cp1.base.NoActiveTransactionException;
import cp1.base.AnotherTransactionActiveException;
import cp1.base.LocalTimeProvider;
import cp1.base.Resource;

public class Transactions {

    private final static long BASE_WAIT_TIME = 500;

    public static void main(String[] args) {
        demo();
        abortingTransactionShouldThrowRightException();
        interruptDuringExecuteShouldReserveResource();
        interruptFlagShouldntBeChangedByCommit();
        interruptFlagShouldntBeChangedByRollback();
        interruptFlagSetInUndoShouldntBeChangedByRollback();
        operationInterruptedDuringExecuteShouldntBeUndoDuringRollback();
        test1();
        testJankowski();
        parallelTest();
        testCritical();
    }

    public static void testCritical() {
        ResourceImpl r = new ResourceImpl(ResourceIdImpl.generate());
        List<Resource> resources = Collections.unmodifiableList(Arrays.asList(r));
        TransactionManager tm = TransactionManagerFactory.newTM(
                resources,
                new LocalTimeProviderImpl());
        List<Thread> threads = new ArrayList<Thread>();
        Runnable workFunc = new Runnable() {
            @Override
            public void run() {
                try {
                    tm.startTransaction();
                    tm.operateOnResourceInCurrentTransaction(
                            r.getId(),
                            CriticalResourceOpImpl.get());
                    tm.commitCurrentTransaction();
                } catch (AnotherTransactionActiveException | NoActiveTransactionException
                        | UnknownResourceIdException | ActiveTransactionAborted | ResourceOperationException
                        | InterruptedException e) {
                    throw new AssertionError(e);
                }
            }
        };
        for (int i = 0; i < 2; ++i) {
            threads.add(new Thread(workFunc));
        }
        for (Thread thread : threads) {
            thread.start();
        }
        try {
            for (Thread t : threads) {
                t.join();
            }
        } catch (InterruptedException e) {
            throw new AssertionError("The main thread has been interrupted");
        }
        expectResourceValue(r, 2);
    }

    public static void parallelTest() {
        ArrayList<ResourceImpl> resources = new ArrayList<ResourceImpl>();
        for (int i = 0; i < 16; ++i) {
            resources.add(new ResourceImpl(ResourceIdImpl.generate()));
        }
        TransactionManager tm = TransactionManagerFactory.newTM(
                Collections.unmodifiableList(resources),
                new LocalTimeProviderImpl());
        final int WAIT_TIME = 1000;

        final class WorkFunc implements Runnable {
            private Resource res;

            public WorkFunc(Resource res) {
                this.res = res;
            }

            @Override
            public void run() {
                try {
                    Thread.sleep(WAIT_TIME);
                    try {
                        tm.startTransaction();
                    } catch (AnotherTransactionActiveException e) {
                        throw new AssertionError(e);
                    }
                    if (!tm.isTransactionActive()) {
                        throw new AssertionError("Failed to start a transaction");
                    }
                    tm.operateOnResourceInCurrentTransaction(
                            res.getId(),
                            ResourceOpImpl.get());
                    tm.commitCurrentTransaction();
                    if (tm.isTransactionActive()) {
                        throw new AssertionError("Failed to commit a transaction");
                    }
                    if (tm.isTransactionAborted()) {
                        throw new AssertionError("Shouldn't be aborted");
                    }
                } catch (UnknownResourceIdException | ResourceOperationException | ActiveTransactionAborted
                        | NoActiveTransactionException | InterruptedException e) {
                    throw new AssertionError(e);
                } finally {
                    tm.rollbackCurrentTransaction();
                }
            }
        }

        List<Thread> threads = new ArrayList<Thread>();
        for (Resource r : resources) {
            threads.add(new Thread(new WorkFunc(r)));
            threads.add(new Thread(new WorkFunc(r)));
        }
        long start = System.nanoTime();
        for (Thread thread : threads) {
            thread.start();
        }
        try {
            for (Thread t : threads) {
                t.join();
            }
        } catch (InterruptedException e) {
            throw new AssertionError("The main thread has been interrupted");
        }
        long end = System.nanoTime();
        long duration = (end - start) / 1000000;
        if (duration >= 2 * WAIT_TIME + 500) {
            throw new AssertionError("Parallel test took too much time");
        }

        for (ResourceImpl r : resources) {
            expectResourceValue(r, 2);
        }
    }

    public static void interruptFlagSetInUndoShouldntBeChangedByRollback() {
        ResourceImpl r = new ResourceImpl(ResourceIdImpl.generate());
        List<Resource> resources = Collections.unmodifiableList(
                Arrays.asList(r));
        TransactionManager tm = TransactionManagerFactory.newTM(
                resources,
                new LocalTimeProviderImpl());
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    try {
                        tm.startTransaction();
                    } catch (AnotherTransactionActiveException e) {
                        throw new AssertionError(e);
                    }
                    if (!tm.isTransactionActive()) {
                        throw new AssertionError("Failed to start a transaction");
                    }
                    tm.operateOnResourceInCurrentTransaction(
                            r.getId(),
                            InterruptingUndoResourceOpImpl.get());

                    Thread self = Thread.currentThread();
                    if (self.isInterrupted()) {
                        throw new AssertionError("Assume start with uninterrupted");
                    }
                    tm.rollbackCurrentTransaction();
                    if (!self.isInterrupted()) {
                        throw new AssertionError("rollback changed interrupt set in undo");
                    }
                    if (tm.isTransactionActive()) {
                        throw new AssertionError("Failed to commit a transaction");
                    }
                    if (tm.isTransactionAborted()) {
                        throw new AssertionError("Shouldn't be aborted");
                    }
                } catch (UnknownResourceIdException | ResourceOperationException | ActiveTransactionAborted
                        | NoActiveTransactionException | InterruptedException e) {
                    throw new AssertionError(e);
                } finally {
                    tm.rollbackCurrentTransaction();
                }
            }
        });
        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new AssertionError("The main thread has been interrupted");
        }
        expectResourceValue(r, 0);
    }

    public static void operationInterruptedDuringExecuteShouldntBeUndoDuringRollback() {
        ResourceImpl r = new ResourceImpl(ResourceIdImpl.generate());
        List<Resource> resources = Collections.unmodifiableList(
                Arrays.asList(r));
        TransactionManager tm = TransactionManagerFactory.newTM(
                resources,
                new LocalTimeProviderImpl());
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    try {
                        tm.startTransaction();
                    } catch (AnotherTransactionActiveException e) {
                        throw new AssertionError(e);
                    }
                    if (!tm.isTransactionActive()) {
                        throw new AssertionError("Failed to start a transaction");
                    }
                    if (Thread.currentThread().isInterrupted()) {
                        throw new AssertionError("Assume uninterrupted at the start");
                    }
                    try {
                        tm.operateOnResourceInCurrentTransaction(
                                r.getId(),
                                InterruptingResourceOpImpl.get());
                    } catch (InterruptedException e) {
                    }

                    if (!Thread.currentThread().isInterrupted()) {
                        throw new AssertionError("Should be interrupted now");
                    }
                    int previous = InterruptingResourceOpImpl.get().getUndoCalls();
                    tm.rollbackCurrentTransaction();
                    if (previous != InterruptingResourceOpImpl.get().getUndoCalls()) {
                        throw new AssertionError(
                                "undo called in rollback even though execute operation was interrupted");
                    }
                    if (tm.isTransactionActive()) {
                        throw new AssertionError("Failed to commit a transaction");
                    }
                    if (tm.isTransactionAborted()) {
                        throw new AssertionError("Shouldn't be aborted");
                    }
                } catch (UnknownResourceIdException | ResourceOperationException | ActiveTransactionAborted
                        | NoActiveTransactionException e) {
                    throw new AssertionError(e);
                }
            }
        });

        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new AssertionError("The main thread has been interrupted");
        }
        expectResourceValue(r, 0);
    }

    public static void interruptDuringExecuteShouldReserveResource() {
        // Set up resources.
        ResourceImpl r1 = new ResourceImpl(ResourceIdImpl.generate());
        ResourceImpl r2 = new ResourceImpl(ResourceIdImpl.generate());
        List<Resource> resources = Collections.unmodifiableList(
                Arrays.asList(r1, r2));
        // Set up a transaction manager.
        TransactionManager tm = TransactionManagerFactory.newTM(
                resources,
                new LocalTimeProviderImpl());
        // Set up threads operation on the resources.
        // Create a cycle, where A owns resource 1 and wants 2, and B owns r2
        // and wants r1. Operation on 1 is interrupted but should leave the
        // resource unchanged and acquired by A, so this should deadlock and
        // B should be aborted as it is started later.
        ArrayList<Thread> threads = new ArrayList<Thread>();
        threads.add(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    try {
                        tm.startTransaction();
                    } catch (AnotherTransactionActiveException e) {
                        throw new AssertionError(e);
                    }
                    if (!tm.isTransactionActive()) {
                        throw new AssertionError("Failed to start a transaction");
                    }
                    boolean exceptionThrown = false;
                    try {
                        tm.operateOnResourceInCurrentTransaction(
                                r1.getId(),
                                InterruptingResourceOpImpl.get());
                    } catch (InterruptedException e) {
                        exceptionThrown = true;
                    }

                    if (!exceptionThrown) {
                        throw new AssertionError("Interrupted operation should throw an exception.");
                    }
                    // Wait until thread B acquires resource 2.
                    Thread.interrupted();
                    try {
                        Thread.sleep(2 * BASE_WAIT_TIME);
                    } catch (InterruptedException e) {
                    }
                    tm.operateOnResourceInCurrentTransaction(
                            r2.getId(),
                            ResourceOpImpl.get());
                    tm.commitCurrentTransaction();
                    if (tm.isTransactionActive()) {
                        throw new AssertionError("Failed to commit a transaction");
                    }
                    if (tm.isTransactionAborted()) {
                        throw new AssertionError("Shouldn't be aborted");
                    }
                } catch (UnknownResourceIdException | ResourceOperationException | ActiveTransactionAborted
                        | NoActiveTransactionException | InterruptedException e) {
                    throw new AssertionError(e);
                } finally {
                    tm.rollbackCurrentTransaction();
                }
            }
        }));

        threads.add(new Thread(new Runnable() {

            @Override
            public void run() {
                // Start later and wait until after transaction A acquired r1.
                try {
                    Thread.sleep(1 * BASE_WAIT_TIME);
                } catch (InterruptedException e) {
                }
                try {
                    tm.startTransaction();
                } catch (AnotherTransactionActiveException e) {
                    throw new AssertionError(e);
                }
                if (!tm.isTransactionActive()) {
                    throw new AssertionError("Failed to start a transaction");
                }
                try {
                    tm.operateOnResourceInCurrentTransaction(
                            r2.getId(),
                            ResourceOpImpl.get());
                    boolean exceptionThrown = false;
                    try {
                        tm.operateOnResourceInCurrentTransaction(
                                r1.getId(),
                                ResourceOpImpl.get());
                    } catch (ActiveTransactionAborted e) {
                        exceptionThrown = true;
                    }
                    if (!exceptionThrown) {
                        throw new AssertionError("Should create a cycle so should be aborted");
                    }
                    if (!tm.isTransactionAborted()) {
                        throw new AssertionError("Should be aborted");
                    }
                } catch (InterruptedException | ActiveTransactionAborted | NoActiveTransactionException
                        | UnknownResourceIdException | ResourceOperationException e) {
                    throw new AssertionError(e);
                } finally {
                    tm.rollbackCurrentTransaction();
                }
            }
        }));

        // Start the threads and wait for them to finish.
        for (Thread t : threads) {
            t.start();
        }
        try {
            for (Thread t : threads) {
                t.join();
            }
        } catch (InterruptedException e) {
            throw new AssertionError("The main thread has been interrupted");
        }

        // `r1` should be unchanged.
        expectResourceValue(r1, 0);
        // `r2` should only be operated by the first thread
        expectResourceValue(r2, 1);
    }

    public static void interruptFlagShouldntBeChangedByCommit() {
        // Set up resources.
        ResourceImpl r = new ResourceImpl(ResourceIdImpl.generate());
        List<Resource> resources = Collections.unmodifiableList(
                Arrays.asList(r));
        // Set up a transaction manager.
        TransactionManager tm = TransactionManagerFactory.newTM(
                resources,
                new LocalTimeProviderImpl());
        // Set up threads operation on the resources.
        ArrayList<Thread> threads = new ArrayList<Thread>();
        threads.add(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    try {
                        tm.startTransaction();
                    } catch (AnotherTransactionActiveException e) {
                        throw new AssertionError(e);
                    }
                    if (!tm.isTransactionActive()) {
                        throw new AssertionError("Failed to start a transaction");
                    }
                    tm.operateOnResourceInCurrentTransaction(
                            r.getId(),
                            ResourceOpImpl.get());

                    Thread self = Thread.currentThread();
                    if (self.isInterrupted()) {
                        throw new AssertionError("Assume the thread is uninterrupted");
                    }
                    tm.commitCurrentTransaction();
                    if (self.isInterrupted()) {
                        throw new AssertionError("Commit changed interrupted flag");
                    }
                    if (tm.isTransactionActive()) {
                        throw new AssertionError("Failed to commit a transaction");
                    }
                    if (tm.isTransactionAborted()) {
                        throw new AssertionError("Shouldn't be aborted");
                    }
                } catch (UnknownResourceIdException | ResourceOperationException | ActiveTransactionAborted
                        | NoActiveTransactionException | InterruptedException e) {
                    throw new AssertionError(e);
                } finally {
                    tm.rollbackCurrentTransaction();
                }
            }
        }));

        threads.add(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    try {
                        tm.startTransaction();
                    } catch (AnotherTransactionActiveException e) {
                        throw new AssertionError(e);
                    }
                    if (!tm.isTransactionActive()) {
                        throw new AssertionError("Failed to start a transaction");
                    }
                    tm.operateOnResourceInCurrentTransaction(
                            r.getId(),
                            ResourceOpImpl.get());

                    Thread self = Thread.currentThread();
                    if (self.isInterrupted()) {
                        throw new AssertionError("Assume the thread is uninterrupted");
                    }
                    self.interrupt();
                    tm.commitCurrentTransaction();
                    if (!self.isInterrupted()) {
                        throw new AssertionError("Commit changed interrupted flag");
                    }
                    if (tm.isTransactionActive()) {
                        throw new AssertionError("Failed to commit a transaction");
                    }
                    if (tm.isTransactionAborted()) {
                        throw new AssertionError("Shouldn't be aborted");
                    }
                } catch (UnknownResourceIdException | ResourceOperationException | ActiveTransactionAborted
                        | NoActiveTransactionException | InterruptedException e) {
                    throw new AssertionError(e);
                } finally {
                    tm.rollbackCurrentTransaction();
                }
            }
        }));

        // Start the threads and wait for them to finish.
        for (Thread t : threads) {
            t.start();
        }
        try {
            for (Thread t : threads) {
                t.join();
            }
        } catch (InterruptedException e) {
            throw new AssertionError("The main thread has been interrupted");
        }
        expectResourceValue(r, 2);
    }

    public static void interruptFlagShouldntBeChangedByRollback() {
        // Set up resources.
        ResourceImpl r = new ResourceImpl(ResourceIdImpl.generate());
        List<Resource> resources = Collections.unmodifiableList(
                Arrays.asList(r));
        // Set up a transaction manager.
        TransactionManager tm = TransactionManagerFactory.newTM(
                resources,
                new LocalTimeProviderImpl());
        // Set up threads operation on the resources.
        ArrayList<Thread> threads = new ArrayList<Thread>();
        threads.add(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    try {
                        tm.startTransaction();
                    } catch (AnotherTransactionActiveException e) {
                        throw new AssertionError(e);
                    }
                    if (!tm.isTransactionActive()) {
                        throw new AssertionError("Failed to start a transaction");
                    }
                    tm.operateOnResourceInCurrentTransaction(
                            r.getId(),
                            ResourceOpImpl.get());

                    Thread self = Thread.currentThread();
                    if (self.isInterrupted()) {
                        throw new AssertionError("Assume the thread is uninterrupted");
                    }
                    tm.rollbackCurrentTransaction();
                    if (self.isInterrupted()) {
                        throw new AssertionError("Rollback changed interrupted flag");
                    }
                    if (tm.isTransactionActive()) {
                        throw new AssertionError("Failed to rollback a transaction");
                    }
                    if (tm.isTransactionAborted()) {
                        throw new AssertionError("Shouldn't be aborted");
                    }
                } catch (UnknownResourceIdException | ResourceOperationException | ActiveTransactionAborted
                        | NoActiveTransactionException | InterruptedException e) {
                    throw new AssertionError(e);
                } finally {
                    tm.rollbackCurrentTransaction();
                }
            }
        }));

        threads.add(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    try {
                        tm.startTransaction();
                    } catch (AnotherTransactionActiveException e) {
                        throw new AssertionError(e);
                    }
                    if (!tm.isTransactionActive()) {
                        throw new AssertionError("Failed to start a transaction");
                    }
                    tm.operateOnResourceInCurrentTransaction(
                            r.getId(),
                            ResourceOpImpl.get());

                    Thread self = Thread.currentThread();
                    if (self.isInterrupted()) {
                        throw new AssertionError("Assume the thread is uninterrupted");
                    }
                    self.interrupt();
                    tm.rollbackCurrentTransaction();
                    if (!self.isInterrupted()) {
                        throw new AssertionError("Rollback changed interrupted flag");
                    }
                    if (tm.isTransactionActive()) {
                        throw new AssertionError("Failed to rollback a transaction");
                    }
                    if (tm.isTransactionAborted()) {
                        throw new AssertionError("Shouldn't be aborted");
                    }
                } catch (UnknownResourceIdException | ResourceOperationException | ActiveTransactionAborted
                        | NoActiveTransactionException | InterruptedException e) {
                    throw new AssertionError(e);
                } finally {
                    tm.rollbackCurrentTransaction();
                }
            }
        }));

        // Start the threads and wait for them to finish.
        for (Thread t : threads) {
            t.start();
        }
        try {
            for (Thread t : threads) {
                t.join();
            }
        } catch (InterruptedException e) {
            throw new AssertionError("The main thread has been interrupted");
        }
        expectResourceValue(r, 0);
    }

    public static void abortingTransactionShouldThrowRightException() {
        // Set up resources.
        ResourceImpl r1 = new ResourceImpl(ResourceIdImpl.generate());
        ResourceImpl r2 = new ResourceImpl(ResourceIdImpl.generate());
        ResourceImpl r3 = new ResourceImpl(ResourceIdImpl.generate());
        List<Resource> resources = Collections.unmodifiableList(
                Arrays.asList(r1, r2, r3));

        // Set up a transaction manager.
        TransactionManager tm = TransactionManagerFactory.newTM(
                resources,
                new LocalTimeProviderImpl());

        // Set up threads operation on the resources.
        // Create a cycle, where n owns resource n but wants n % 3 + 1.
        // Run thread 3 latest so that it gets aborted.
        ArrayList<Thread> threads = new ArrayList<Thread>();
        threads.add(
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            tm.startTransaction();
                        } catch (AnotherTransactionActiveException e) {
                            throw new AssertionError(e);
                        }
                        if (!tm.isTransactionActive()) {
                            throw new AssertionError("Failed to start a transaction");
                        }
                        try {
                            tm.operateOnResourceInCurrentTransaction(
                                    r1.getId(),
                                    ResourceOpImpl.get());
                            // Wait until after the second transaction acquired r2.
                            Thread.sleep(2 * BASE_WAIT_TIME);
                            tm.operateOnResourceInCurrentTransaction(
                                    r2.getId(),
                                    ResourceOpImpl.get());
                            tm.commitCurrentTransaction();
                            if (tm.isTransactionActive()) {
                                throw new AssertionError("Failed to commit a transaction");
                            }
                            if (tm.isTransactionAborted()) {
                                throw new AssertionError("Shouldn't be aborted");
                            }
                        } catch (InterruptedException | ActiveTransactionAborted | NoActiveTransactionException
                                | UnknownResourceIdException | ResourceOperationException e) {
                            throw new AssertionError(e);
                        } finally {
                            tm.rollbackCurrentTransaction();
                        }
                    }
                }));
        threads.add(
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        // This transaction should be later than the first.
                        try {
                            Thread.sleep(1 * BASE_WAIT_TIME);
                        } catch (InterruptedException e) {
                        }
                        try {
                            tm.startTransaction();
                        } catch (AnotherTransactionActiveException e) {
                            throw new AssertionError(e);
                        }
                        if (!tm.isTransactionActive()) {
                            throw new AssertionError("Failed to start a transaction");
                        }
                        try {
                            tm.operateOnResourceInCurrentTransaction(
                                    r2.getId(),
                                    ResourceOpImpl.get());
                            // Wait until after the third transaction acquired r3.
                            Thread.sleep(2 * BASE_WAIT_TIME);
                            tm.operateOnResourceInCurrentTransaction(
                                    r3.getId(),
                                    ResourceOpImpl.get());
                            tm.commitCurrentTransaction();
                            if (tm.isTransactionActive()) {
                                throw new AssertionError("Failed to commit a transaction");
                            }
                            if (tm.isTransactionAborted()) {
                                throw new AssertionError("Shouldn't be aborted");
                            }
                        } catch (InterruptedException | ActiveTransactionAborted | NoActiveTransactionException
                                | UnknownResourceIdException | ResourceOperationException e) {
                            throw new AssertionError(e);
                        } finally {
                            tm.rollbackCurrentTransaction();
                        }
                    }
                }));
        threads.add(
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        // This transaction should be the latest.
                        try {
                            Thread.sleep(2 * BASE_WAIT_TIME);
                        } catch (InterruptedException e) {
                        }
                        try {
                            tm.startTransaction();
                        } catch (AnotherTransactionActiveException e) {
                            throw new AssertionError(e);
                        }
                        if (!tm.isTransactionActive()) {
                            throw new AssertionError("Failed to start a transaction");
                        }
                        try {
                            tm.operateOnResourceInCurrentTransaction(
                                    r3.getId(),
                                    ResourceOpImpl.get());
                            // This should not succeed because of a deadlock.
                            boolean aborted = false;
                            try {
                                tm.operateOnResourceInCurrentTransaction(
                                        r1.getId(),
                                        ResourceOpImpl.get());
                            } catch (ActiveTransactionAborted e) {
                                aborted = true;
                            }
                            if (!aborted) {
                                throw new AssertionError("Latest should be aborted when in deadlock");
                            }
                            aborted = false;
                            try {
                                tm.commitCurrentTransaction();
                            } catch (ActiveTransactionAborted e) {
                                aborted = true;
                            }
                            if (!aborted) {
                                throw new AssertionError("Latest should be aborted when in deadlock");
                            }
                            if (!tm.isTransactionActive()) {
                                throw new AssertionError("Aborted transaction but still not finished");
                            }
                            if (!tm.isTransactionAborted()) {
                                throw new AssertionError("Should be aborted");
                            }
                        } catch (InterruptedException | ActiveTransactionAborted | NoActiveTransactionException
                                | UnknownResourceIdException | ResourceOperationException e) {
                            throw new AssertionError(e);
                        } finally {
                            tm.rollbackCurrentTransaction();
                        }
                    }
                }));

        // Start the threads and wait for them to finish.
        for (Thread t : threads) {
            t.start();
        }
        try {
            for (Thread t : threads) {
                t.join();
            }
        } catch (InterruptedException e) {
            throw new AssertionError("The main thread has been interrupted");
        }
        // Check the results. Only the first and second threads' transactions
        // should have succeeded.
        expectResourceValue(r1, 1);
        expectResourceValue(r2, 2);
        expectResourceValue(r3, 1);
    }

    public static void demo() {
        // Set up resources.
        ResourceImpl r1 = new ResourceImpl(ResourceIdImpl.generate());
        ResourceImpl r2 = new ResourceImpl(ResourceIdImpl.generate());
        ResourceImpl r3 = new ResourceImpl(ResourceIdImpl.generate());
        ResourceImpl r4 = new ResourceImpl(ResourceIdImpl.generate());
        List<Resource> resources = Collections.unmodifiableList(
                Arrays.asList(r1, r2, r3, r4));

        // Set up a transaction manager.
        TransactionManager tm = TransactionManagerFactory.newTM(
                resources,
                new LocalTimeProviderImpl());

        // Set up threads operation on the resources.
        ArrayList<Thread> threads = new ArrayList<Thread>();
        threads.add(
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Thread.sleep(1 * BASE_WAIT_TIME);
                        } catch (InterruptedException e) {
                        }
                        try {
                            tm.startTransaction();
                        } catch (AnotherTransactionActiveException e) {
                            throw new AssertionError(e);
                        }
                        if (!tm.isTransactionActive()) {
                            throw new AssertionError("Failed to start a transaction");
                        }
                        try {
                            tm.operateOnResourceInCurrentTransaction(
                                    r1.getId(),
                                    ResourceOpImpl.get());
                            Thread.sleep(4 * BASE_WAIT_TIME);
                            tm.commitCurrentTransaction();
                            if (tm.isTransactionActive()) {
                                throw new AssertionError("Failed to commit a transaction");
                            }
                            if (tm.isTransactionAborted()) {
                                throw new AssertionError("Shouldn't be aborted");
                            }
                        } catch (InterruptedException | ActiveTransactionAborted | NoActiveTransactionException
                                | UnknownResourceIdException | ResourceOperationException e) {
                            throw new AssertionError(e);
                        } finally {
                            tm.rollbackCurrentTransaction();
                        }
                    }
                }));
        threads.add(
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Thread.sleep(1 * BASE_WAIT_TIME);
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
                        try {
                            tm.startTransaction();
                        } catch (AnotherTransactionActiveException e) {
                            throw new AssertionError(e);
                        }
                        if (!tm.isTransactionActive()) {
                            throw new AssertionError("Failed to start a transaction");
                        }
                        try {
                            tm.operateOnResourceInCurrentTransaction(
                                    r2.getId(),
                                    ResourceOpImpl.get());
                            Thread.sleep(6 * BASE_WAIT_TIME);
                            tm.operateOnResourceInCurrentTransaction(
                                    r3.getId(),
                                    ResourceOpImpl.get());
                            Thread.sleep(1 * BASE_WAIT_TIME);
                        } catch (InterruptedException | ActiveTransactionAborted | NoActiveTransactionException
                                | ResourceOperationException | UnknownResourceIdException e) {
                            throw new AssertionError(e);
                        } finally {
                            tm.rollbackCurrentTransaction();
                            if (tm.isTransactionActive()) {
                                throw new AssertionError("Failed to rollback a transaction");
                            }
                        }
                    }
                }));
        threads.add(
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Thread.sleep(2 * BASE_WAIT_TIME);
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
                        try {
                            tm.startTransaction();
                        } catch (AnotherTransactionActiveException e) {
                            throw new AssertionError(e);
                        }
                        if (!tm.isTransactionActive()) {
                            throw new AssertionError("Failed to start a transaction");
                        }
                        if (tm.isTransactionAborted()) {
                            throw new AssertionError("Invalid transaction state");
                        }
                        try {
                            tm.operateOnResourceInCurrentTransaction(
                                    r2.getId(),
                                    ResourceOpImpl.get());
                            Thread.sleep(1 * BASE_WAIT_TIME);
                            tm.operateOnResourceInCurrentTransaction(
                                    r1.getId(),
                                    ResourceOpImpl.get());
                            tm.commitCurrentTransaction();
                            if (tm.isTransactionActive()) {
                                throw new AssertionError("Failed to commit a transaction");
                            }
                        } catch (InterruptedException | ActiveTransactionAborted | NoActiveTransactionException
                                | ResourceOperationException | UnknownResourceIdException e) {
                            throw new AssertionError(e);
                        } finally {
                            tm.rollbackCurrentTransaction();
                        }
                    }
                }));
        // Start the threads and wait for them to finish.
        for (Thread t : threads) {
            t.start();
        }
        try {
            for (Thread t : threads) {
                t.join();
            }
        } catch (InterruptedException e) {
            throw new AssertionError("The main thread has been interrupted");
        }
        // Check the results.
        expectResourceValue(r1, 2);
        expectResourceValue(r2, 1);
        expectResourceValue(r3, 0);
    }

    public static void test1() {
        // Set up resources.
        ResourceImpl r1 = new ResourceImpl(ResourceIdImpl.generate());
        ResourceImpl r2 = new ResourceImpl(ResourceIdImpl.generate());
        ResourceImpl r3 = new ResourceImpl(ResourceIdImpl.generate());
        ResourceImpl r4 = new ResourceImpl(ResourceIdImpl.generate());
        List<Resource> resources = Collections.unmodifiableList(
                Arrays.asList(r1, r2, r3, r4));

        // Set up a transaction manager.
        TransactionManager tm = TransactionManagerFactory.newTM(
                resources,
                new LocalTimeProviderImpl());

        // Set up threads operation on the resources.
        ArrayList<Thread> threads = new ArrayList<Thread>();
        threads.add(
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            tm.startTransaction();
                        } catch (AnotherTransactionActiveException e) {
                            throw new AssertionError(e);
                        }
                        if (!tm.isTransactionActive()) {
                            throw new AssertionError("Failed to start a transaction");
                        }
                        try {
                            System.out.println(Thread.currentThread().getId() + " Enter Resource 1");
                            tm.operateOnResourceInCurrentTransaction(
                                    r1.getId(),
                                    ResourceOpImpl.get());
                            System.out.println(Thread.currentThread().getId() + " Exit Resource 1");
                            Thread.sleep(10 * BASE_WAIT_TIME);

                            System.out.println(Thread.currentThread().getId() + " Enter Resource 2");
                            tm.operateOnResourceInCurrentTransaction(
                                    r2.getId(),
                                    ResourceOpImpl.get());
                            System.out.println(Thread.currentThread().getId() + " Exit Resource 2");
                            Thread.sleep(2 * BASE_WAIT_TIME);

                            tm.commitCurrentTransaction();
                            if (tm.isTransactionActive()) {
                                throw new AssertionError("Failed to commit a transaction");
                            }
                        } catch (InterruptedException | ActiveTransactionAborted | NoActiveTransactionException
                                | UnknownResourceIdException | ResourceOperationException e) {
                            throw new AssertionError(e);
                        } finally {
                            tm.rollbackCurrentTransaction();
                        }
                    }
                }));
        threads.add(
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            tm.startTransaction();
                        } catch (AnotherTransactionActiveException e) {
                            throw new AssertionError(e);
                        }
                        if (!tm.isTransactionActive()) {
                            throw new AssertionError("Failed to start a transaction");
                        }
                        try {
                            Thread.sleep(2 * BASE_WAIT_TIME);
                            System.out.println(Thread.currentThread().getId() + " Enter Resource 2");
                            tm.operateOnResourceInCurrentTransaction(
                                    r2.getId(),
                                    ResourceOpImpl.get());
                            System.out.println(Thread.currentThread().getId() + " Exit Resource 2");
                            System.out.println(Thread.currentThread().getId() + "Enter Resource 1");
                            Thread.sleep(2 * BASE_WAIT_TIME);
                            tm.operateOnResourceInCurrentTransaction(
                                    r1.getId(),
                                    ResourceOpImpl.get());
                            System.out.println(Thread.currentThread().getId() + "Exit Resource 1");
                        } catch (InterruptedException | ActiveTransactionAborted | NoActiveTransactionException
                                | ResourceOperationException | UnknownResourceIdException e) {
                            System.out.println(Thread.currentThread().getId() + " Wątek przerwany");
                        } finally {
                            tm.rollbackCurrentTransaction();
                            if (tm.isTransactionActive()) {
                                throw new AssertionError("Failed to rollback a transaction");
                            }
                        }
                    }
                }));

        // Start the threads and wait for them to finish.
        for (Thread t : threads) {
            t.start();
        }
        try {
            for (Thread t : threads) {
                t.join();
            }
        } catch (InterruptedException e) {
            throw new AssertionError("The main thread has been interrupted");
        }
    }

    /**
     * Author : Krzysztof Jankowski
     *
     * Sposób użytkowania:
     * W pętli nieskończonej wypisują się kolejne wartości licznika.
     * Nie powinno być błędów.
     * UWAGA: LICZBY WYPISUJĄ SIĘ NAPRAWDĘ WOLNO. PROSZĘ CIERPLIWIE CZEKAĆ. CHYBA ŻE
     * JEST DEADLOCK, TO NIE CZEKAĆ.
     *
     * Działanie:
     * 20 wątków losowo zajmuje ustalone zasoby oraz czasami losowe zasoby. co jakiś
     * czas pewien wątek
     * jest przerywany.
     *
     * UWAGA2: NIE WIEM CO MOŻE ZDARZYĆ SIĘ W TRAKCIE, BO TEST JEST MOCNO LOSOWY.
     *
     * Można zmienić N na więcej wątków lub mniej jeśi się chce.
     * Dodatkowo wypisują się kropki, żeby nie czekać bezsensownie na deadlocka.
     */
    public static void testJankowski() {

        for (int licznik = 0; licznik < 5; ++licznik) {
            System.out.println(licznik);
            // Set up resources.
            ResourceImpl r0 = new ResourceImpl(ResourceIdImpl.generate());
            ResourceImpl r1 = new ResourceImpl(ResourceIdImpl.generate());
            ResourceImpl r2 = new ResourceImpl(ResourceIdImpl.generate());
            ResourceImpl r3 = new ResourceImpl(ResourceIdImpl.generate());
            List<Resource> resources = Collections.unmodifiableList(
                    Arrays.asList(r0, r1, r2, r3));

            // Set up a transaction manager.
            TransactionManager tm = TransactionManagerFactory.newTM(
                    resources,
                    new LocalTimeProviderImpl());

            // Set up threads operation on the resources.
            ArrayList<Thread> threads = new ArrayList<Thread>();
            Random r = new Random();
            int N = 20;
            for (int i = 0; i < N; i++) {
                threads.add(
                        new Thread(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    Thread.sleep(r.nextInt(3) * BASE_WAIT_TIME);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                                // Thread-0 starts
                                System.out.print(".");
                                try {
                                    tm.startTransaction();
                                } catch (AnotherTransactionActiveException e) {
                                    // throw new AssertionError(e);
                                }
                                // Thread-0 gets to R0
                                try {
                                    tm.operateOnResourceInCurrentTransaction(
                                            r0.getId(),
                                            ResourceOpImpl.get());
                                    Thread.sleep(r.nextInt(4) * BASE_WAIT_TIME);
                                    // Thread-0 waits on R1
                                    int interrupt = r.nextInt(10);
                                    if (interrupt < 3)
                                        Thread.currentThread().interrupt();
                                    tm.operateOnResourceInCurrentTransaction(
                                            r1.getId(),
                                            ResourceOpImpl.get());
                                    System.out.print(".");
                                    Thread.sleep(r.nextInt(4) * BASE_WAIT_TIME);
                                    tm.operateOnResourceInCurrentTransaction(
                                            r2.getId(),
                                            ResourceOpImpl.get());
                                    Thread.sleep(r.nextInt(4) * BASE_WAIT_TIME);
                                    // Thread-0 waits on R1
                                    tm.operateOnResourceInCurrentTransaction(
                                            r3.getId(),
                                            ResourceOpImpl.get());

                                    interrupt = r.nextInt(10);
                                    if (interrupt < 5)
                                        Thread.currentThread().interrupt();

                                    Thread.sleep(r.nextInt(3) * BASE_WAIT_TIME);
                                    Resource rr = resources.get(r.nextInt(3));
                                    tm.operateOnResourceInCurrentTransaction(
                                            rr.getId(),
                                            ResourceOpImpl.get());
                                    Resource rrr = resources.get(r.nextInt(3));
                                    Thread.sleep(r.nextInt(3) * BASE_WAIT_TIME);
                                    tm.operateOnResourceInCurrentTransaction(
                                            rrr.getId(),
                                            ResourceOpImpl.get());
                                    Thread.sleep(r.nextInt(4) * BASE_WAIT_TIME);
                                    Thread.currentThread().interrupt();
                                    interrupt = r.nextInt(10);
                                    if (interrupt < 3)
                                        Thread.currentThread().interrupt();
                                    tm.commitCurrentTransaction();
                                    if (tm.isTransactionActive()) {
                                        throw new AssertionError("Failed to commit a transaction");
                                    }
                                } catch (InterruptedException | ActiveTransactionAborted | NoActiveTransactionException
                                        | UnknownResourceIdException | ResourceOperationException e) {
                                    // throw new AssertionError(e);
                                } finally {
                                    tm.rollbackCurrentTransaction();
                                }
                            }
                        }));
            }
            // Start the threads and wait for them to finish.
            for (Thread t : threads) {
                t.start();
            }
            try {
                for (Thread t : threads) {
                    t.join();
                }
            } catch (InterruptedException e) {
                throw new AssertionError("The main thread has been interrupted");
            }
            // Check the results.
            // expectResourceValue(r1, 2);
            // expectResourceValue(r2, 1);
            // expectResourceValue(r3, 0);
        }
    }

    private final static void expectResourceValue(ResourceImpl r, long val) {
        if (r.getValue() != val) {
            throw new AssertionError(
                    "For resource " + r.getId() +
                            ", expected value " + val +
                            ", but got value " + r.getValue());
        }
    }

    // ---------------------------------------------------------
    // - -
    // - Sample implementations of the base interfaces -
    // - -
    // ---------------------------------------------------------

    private static final class LocalTimeProviderImpl implements LocalTimeProvider {
        @Override
        public long getTime() {
            return System.currentTimeMillis();
        }
    }

    private static final class ResourceIdImpl implements ResourceId {
        private static volatile int next = 0;

        public static synchronized ResourceId generate() {
            return new ResourceIdImpl(next++);
        }

        private final int value;

        private ResourceIdImpl(int value) {
            this.value = value;
        }

        @Override
        public int compareTo(ResourceId other) {
            if (!(other instanceof ResourceIdImpl)) {
                throw new RuntimeException("Comparing incompatible resource IDs");
            }
            ResourceIdImpl second = (ResourceIdImpl) other;
            return Integer.compare(this.value, second.value);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof ResourceIdImpl)) {
                return false;
            }
            ResourceIdImpl second = (ResourceIdImpl) obj;
            return this.value == second.value;
        }

        @Override
        public int hashCode() {
            return Integer.hashCode(this.value);
        }

        @Override
        public String toString() {
            return "R" + this.value;
        }
    }

    private static final class ResourceImpl extends Resource {
        private volatile long value = 0;

        public ResourceImpl(ResourceId id) {
            super(id);
        }

        public void incValue() {
            long x = this.value;
            ++x;
            this.value = x;
        }

        public void decValue() {
            long x = this.value;
            --x;
            this.value = x;
        }

        public long getValue() {
            return this.value;
        }
    }

    private static final class ResourceOpImpl extends ResourceOperation {
        private final static ResourceOpImpl singleton = new ResourceOpImpl();

        public static ResourceOperation get() {
            return singleton;
        }

        private ResourceOpImpl() {
        }

        @Override
        public String toString() {
            return "OP_" + super.toString();
        }

        @Override
        public void execute(Resource r) {
            if (!(r instanceof ResourceImpl)) {
                throw new AssertionError("Unexpected resource type " +
                        r.getClass().getCanonicalName());
            }
            ((ResourceImpl) r).incValue();
        }

        @Override
        public void undo(Resource r) {
            if (!(r instanceof ResourceImpl)) {
                throw new AssertionError("Unexpected resource type " +
                        r.getClass().getCanonicalName());
            }
            ((ResourceImpl) r).decValue();
        }
    }

    private static final class CriticalResourceOpImpl extends ResourceOperation {
        private final static CriticalResourceOpImpl singleton = new CriticalResourceOpImpl();
        private AtomicInteger thread_count;

        public static ResourceOperation get() {
            return singleton;
        }

        private CriticalResourceOpImpl() {
            thread_count = new AtomicInteger();
        }

        @Override
        public String toString() {
            return "OP_" + super.toString();
        }

        @Override
        public void execute(Resource r) {
            if (thread_count.incrementAndGet() != 1) {
                throw new AssertionError("More than 1 thread in critical section");
            }
            try {
                Thread.sleep(2 * BASE_WAIT_TIME);
            } catch (InterruptedException e) {
                throw new AssertionError("Sleep shouldn't throw in the test");
            }
            if (!(r instanceof ResourceImpl)) {
                throw new AssertionError("Unexpected resource type " +
                        r.getClass().getCanonicalName());
            }
            ((ResourceImpl) r).incValue();
            thread_count.decrementAndGet();
        }

        @Override
        public void undo(Resource r) {
            if (!(r instanceof ResourceImpl)) {
                throw new AssertionError("Unexpected resource type " +
                        r.getClass().getCanonicalName());
            }
            ((ResourceImpl) r).decValue();
        }
    }

    private static final class InterruptingResourceOpImpl extends ResourceOperation {
        private final static InterruptingResourceOpImpl singleton = new InterruptingResourceOpImpl();
        private int undoCalls;

        public static InterruptingResourceOpImpl get() {
            return singleton;
        }

        private InterruptingResourceOpImpl() {
            undoCalls = 0;
        }

        public int getUndoCalls() {
            return undoCalls;
        }

        @Override
        public String toString() {
            return "Interrupting_OP_" + super.toString();
        }

        @Override
        public void execute(Resource r) {
            if (!(r instanceof ResourceImpl)) {
                throw new AssertionError("Unexpected resource type " +
                        r.getClass().getCanonicalName());
            }
            Thread.currentThread().interrupt();
            ((ResourceImpl) r).incValue();
        }

        @Override
        public void undo(Resource r) {
            ++undoCalls;
            if (!(r instanceof ResourceImpl)) {
                throw new AssertionError("Unexpected resource type " +
                        r.getClass().getCanonicalName());
            }
            ((ResourceImpl) r).decValue();
        }
    }

    private static final class InterruptingUndoResourceOpImpl extends ResourceOperation {
        private final static InterruptingUndoResourceOpImpl singleton = new InterruptingUndoResourceOpImpl();

        public static ResourceOperation get() {
            return singleton;
        }

        private InterruptingUndoResourceOpImpl() {
        }

        @Override
        public String toString() {
            return "Interrupting_Undo_OP_" + super.toString();
        }

        @Override
        public void execute(Resource r) {
            if (!(r instanceof ResourceImpl)) {
                throw new AssertionError("Unexpected resource type " +
                        r.getClass().getCanonicalName());
            }
            ((ResourceImpl) r).incValue();
        }

        @Override
        public void undo(Resource r) {
            if (!(r instanceof ResourceImpl)) {
                throw new AssertionError("Unexpected resource type " +
                        r.getClass().getCanonicalName());
            }
            Thread.currentThread().interrupt();
            ((ResourceImpl) r).decValue();
        }
    }
}
