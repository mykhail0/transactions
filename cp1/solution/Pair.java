/*
 * University of Warsaw
 * Concurrent Programming Course 2020/2021
 * Java Assignment
 * 
 * Author: Mykhailo Shevchenko (ms420826@students.mimuw.edu.pl)
 */
package cp1.solution;

public class Pair<A, B> {
    private A first;
    private B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public A getFirst() {
        return first;
    }

    public B getSecond() {
        return second;
    }
}
