package org.mem.store.query.model;

/**
 * Created by IntelliJ IDEA.
 * User: aathalye
 * Date: 10/12/13
 * Time: 10:30 AM
 *
 * Logical predicate is obtained by performing logical
 * operation between any 2 child predicates.
 * e.g : a > 5 and b < 3
 * <p>
 *     Nesting of predicates can be obtained by combining
 *     logical predicates using logical operators.
 * </p>
 *
 */
public interface LogicalPredicate extends Predicate {

    /**
     * Logical predicate only works on 2 child predicates at a time.
     * @param predicate1
     * @param predicate2
     */
    <P extends Predicate> void setChildPredicates(P predicate1, P predicate2);

    /**
     *
     * @return
     */
    <P extends Predicate> P[] getChildPredicates();
}
