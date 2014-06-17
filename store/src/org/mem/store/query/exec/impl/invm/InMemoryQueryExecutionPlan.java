package org.mem.store.query.exec.impl.invm;

import org.mem.store.query.exec.PredicateEvaluationTreeVisitor;
import org.mem.store.query.exec.PredicateEvaluator;
import org.mem.store.query.exec.QueryExecutionPlan;
import org.mem.store.query.model.impl.MutableResultStream;

/**
 * Created by IntelliJ IDEA.
 * User: aathalye
 * Date: 27/11/13
 * Time: 12:05 PM
 *
 * In Memory simple representation of a query execution plan.
 */
public class InMemoryQueryExecutionPlan implements QueryExecutionPlan {

    /**
     * Execution plan will evaluate context followed by predicate evaluation followed
     * by misc like order by etc.
     */
    private SimplePredicateEvaluationTree predicateEvaluationTree;


    public <P extends PredicateEvaluator> void addEvaluator(P predicateEvaluator) {
        if (predicateEvaluationTree == null) {
            predicateEvaluationTree = new SimplePredicateEvaluationTree();
        }
        predicateEvaluationTree.addEvaluator(predicateEvaluator);
    }

    @Override
    @SuppressWarnings("unchecked")
    public MutableResultStream execute(PredicateEvaluationTreeVisitor predicateEvaluationTreeVisitor) {
        return predicateEvaluationTree.accept(predicateEvaluationTreeVisitor);
    }
}
