package org.mem.store.query.exec;

/**
 * Created by IntelliJ IDEA.
 * User: aathalye
 * Date: 17/12/13
 * Time: 10:57 AM
 *
 * Evaluate binary form expressions like a = 10 or b > 20 etc.
 */
public interface BinaryPredicateEvaluator<P extends PredicateEvaluator> extends PredicateEvaluator {

    public P getLeftChild();

    public P getRightChild();
}
