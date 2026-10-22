# transactions

## Introduction

Transaction is a sequence of operations which are either all to be executed or
none of them at all (atomicity), and that results of intermediate operations are
not visible between different transactions (isolation). This project implements
a concurrent multithreaded transaction manager in Java 11 which was an
assignment from classes on concurrent programming at the University of Warsaw.

To run the demo you should first compile using [compile.sh](compile.sh) and then
`java cp1.demo.Transactions`. To test you can run
`java -ea cp1.tests.Transactions`.

## Specification

We consider a system with a certain unknown number of threads. Threads operate
on a certain collection of resources (objects inheriting after
[`cp1.base.Resource`](cp1/base/Resource.java)), each of which has a constant
unique ID ([`cp1.base.ResourceId`](cp1/base/ResourceId.java)). As a result of
such operations (represented by objects implementing
[`cp1.base.ResourceId`](cp1/base/ResourceId.java) interface) the collection of
resources does not change. However, states of select resources may change.

Operations on resources are executed under transactions. In any moment a given
thread may be executing at most one transaction. Transactions are coordinated by
a transaction manager (implementing
[`cp1.base.TransactionManager`](cp1/base/TransactionManager.java) interface).

During creation (`newTM` method from
[`cp1.solution.TransactionManagerFactory`](cp1/solution/TransactionManagerFactory.java))
a transaction manager receives exclusive ownership over a collection of
resources, as long as an interface for asking current time, necessary for
managing transactions.

### Transaction start

Before a thread executes any operation on a resource, it must start (activate) a
transaction by calling transaction manager's (TM's) `startTransaction`.
Transaction start does not succeed if and only if the thread calling the method
hasn't yet finished a previous transaction. In such case
[`cp1.base.AnotherTransactionActiveException`](cp1/base/AnotherTransactionActiveException.java)
is thrown. The thread may then finish the current transaction and then try again
to start a new transaction. However, if the start succeeded, then the
transaction remains active until a moment it is finished as described below.

### Operations under a transaction

A thread executes an operation on a resource by calling TM's method
`operateOnResourceInCurrentTransaction`, which takes the resource's ID and an
operation to execute as arguments. If the thread does not have an active
transaction, this method call throws
[`cp1.base.NoActiveTransactionException`](cp1/base/NoActiveTransactionException.java).
Similarly, this method, when called with a resource ID which is not managed by
the TM, throws
[`cp1.base.UnknownResourceIdException`](cp1/base/UnknownResourceIdException.java)
with this unknown ID as the argument. If an active transaction has been
previously aborted, as explained further, the method call throws
[`cp1.base.ActiveTransactionAborted`](cp1/base/ActiveTransactionAborted.java).

Otherwise, the thread requests access to the resource. If the access has been
granted already under the active transaction, meaning that the thread has
executed an operation on the resource at least once, the access is granted
immediately. Otherwise, a check is performed if another active transaction with
the access to the resource exists. If such a transaction does not exist, the
access is granted to the requesting thread. Otherwise, the requesting thread
must potentially wait until the transaction with the access finishes.

If such waiting would cause a deadlock for a collection of transactions, TM
aborts the transaction which has started the latest. The start time is measured
using `getTime` method of
[`cp1.base.LocalTimeProvider`](cp1/base/LocalTimeProvider.java) interface
(passed to the TM upon its creation). In the case, when a few transactions have
the same start time, IDs of threads responsible for these transactions decide
which transaction is aborted (`getId` method of `Thread`), meaning that the
transaction of a thread with the highest ID is determined to have the latest
start among all transactions with the same start time. Aborting a transaction
also means interrupting the thread which started it (`interrupt` method of
`Thread`).

An aborted transaction cannot be continued, meaning it can only be finished, as
explained further. In particular, as mentioned earlier, an attempt to execute
any operation under such transaction throws
[`cp1.base.ActiveTransactionAborted`](cp1/base/ActiveTransactionAborted.java).

When a thread has access to a resource, TM starts executing the operation on the
resource passed to it as the `operateOnResourceInCurrentTransaction` method's
argument by calling the operation's `execute` method of the
[`cp1.base.ResourceOperation`](cp1/base/ResourceOperation.java) interface with
the resource as the argument. The execution must be done inside the thread which
ordered the operation, meaning that no other thread can call `execute` on the
operation object passed to TM by a different thread.

An operation may throw
[`cp1.base.ResourceOperationException`](cp1/base/ResourceOperationException.java),
which must be propagated to the caller thread by the TM. In such a case, the
operation was unsuccessful. `execute` method of an unsuccessful operation
guarantees that the resource stays in the same state as before the call.

If an operation did not throw such an exception, it means the operation was
successful. A successful operation may modify a resource's state.

Independent of the success, an operation may be executed by a thread multiple
times, on the same resource or different ones. Furthermore, different executions
may have different results.

During a wait for a given resource, or while executing an operation on it, a
thread may be interrupted (by an `interrupt` call). In such a case,
`operateOnResourceInCurrentTransaction` throws `InterruptedException`, leaving
the resource in an unchanged state, as well as the transaction. In particular,
depending on the moment when the interruption occured, the transaction may or
may not acquire access to the resource. More precisely, if the interruption
occured before or during the process of granting access, the transaction does
not acquire access. Otherwise, the access is granted.

### Transaction finish

A thread may finish an active transaction in two ways:

- by committing (calling `commitCurrentTransaction` method of TM)
- by performing a rollback (calling `rollbackCurrentTransaction` method of TM).

Committing a transaction causes that all resource's modifications by TM are made
visible to all the other transactions. Committing a transaction fails only in
two cases:

1) the thread does not have an active transaction,
[`cp1.base.NoActiveTransactionException`](cp1/base/NoActiveTransactionException.java)
is thrown,
2) the thread's active transaction was aborted,
[`cp1.base.ActiveTransactionAborted`](cp1/base/ActiveTransactionAborted.java)
is thrown and the transaction is not finished.

Performing a transaction's rollback causes that any resource modification
executed by TM with operations under this transaction are undone, meaning that
the states of the affected resources is the same as before the start of the
transaction. Rollbacks always succeed. In particular, when a thread does not
have an active transaction, rollback is a NOP.

Undoing the modifications by a given operation on a given resource is done with
a call to to the operation's `undo` method with the resource as the argument.
Similarly to the `execute` method, `undo` must be called by the thread which
ordered the operation. In contrast to `execute`, `undo` does not throw
exceptions. Only successful operations are undone, and they are undone in a
reverse order of their initial execution under the transaction.

Finishing a transaction in both of the available ways causes TM to stop
storing information about this transaction.

### Checking transaction's state

TM offers two additional methods for a thread to check the state of its
transaction. `isTransactionActive` method called by a thread returns `true` if
and only if the thread has an active transaction. `isTransactionAborted` method
returns `true` if and only if the thread has an active transaction which was
aborted.
