/*
 * University of Warsaw
 * Concurrent Programming Course 2020/2021
 * Java Assignment
 * 
 * Author: Mykhailo Shevchenko (ms420826@students.mimuw.edu.pl)
 */
package cp1.solution;

import cp1.base.ResourceId;
import cp1.base.Resource;
import cp1.base.ResourceOperation;

import java.util.Deque;
import java.util.Map;
import java.util.LinkedList;

public class Transaction {

    private long time;
    private Deque<Pair<ResourceOperation, ResourceId>> operations;
    private Map<ResourceId, Resource> resources;

    public Transaction(long time, Map<ResourceId, Resource> resources) {
        this.time = time;
        operations = new LinkedList<Pair<ResourceOperation, ResourceId>>();
        this.resources = resources;
    }

    // Log a successful operation.
    public void operated(ResourceOperation op, ResourceId id) {
        operations.push(new Pair<ResourceOperation, ResourceId>(op, id));
    }

    public void rollback() {
        while (!operations.isEmpty()) {
            Pair<ResourceOperation, ResourceId> p = operations.pop();
            p.getFirst().undo(resources.get(p.getSecond()));
        }
    }

    public long getTime() {
        return time;
    }

}
