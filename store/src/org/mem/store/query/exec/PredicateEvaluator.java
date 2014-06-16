package org.mem.store.query.exec;

import org.mem.store.query.model.Predicate;
import org.mem.store.query.model.ResultStream;

/**
 * Created by IntelliJ IDEA.
 * User: aathalye
 * Date: 10/12/13
 * Time: 10:18 AM
 * <p/>
 * Generic interface for predicate evaluation in plan tree.
 */
public interface PredicateEvaluator {

    /**
     * Evaluate the wrapped predicate and return the result stream accordingly.
     * Can optionally take result streams as input.
     * Source tables contains array of tables to be queried.
     */
    public <R extends ResultStream> R eval(R... inputResultStreams);


    public boolean addChildPredicateEvaluator(PredicateEvaluator childPredicateEvaluator);

    /**
     * If both left and right children are non-null return true.
     */
    public boolean isFull();


    public <P extends Predicate> P getWrappedPredicate();

    /**
     * Number of operands required by this evaluator
     */
    public int getNumOperands();
}
