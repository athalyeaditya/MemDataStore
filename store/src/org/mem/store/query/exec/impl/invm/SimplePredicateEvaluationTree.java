package org.mem.store.query.exec.impl.invm;

import org.mem.store.query.exec.BinaryPredicateEvaluator;
import org.mem.store.query.exec.FilterPredicateEvaluator;
import org.mem.store.query.exec.JoinPredicateEvaluator;
import org.mem.store.query.exec.PredicateEvaluationTree;
import org.mem.store.query.exec.PredicateEvaluationTreeVisitor;
import org.mem.store.query.exec.PredicateEvaluator;
import org.mem.store.query.exec.RelationalPredicateEvaluator;
import org.mem.store.query.model.ResultStream;

/**
 * Created by IntelliJ IDEA.
 * User: aathalye
 * Date: 12/12/13
 * Time: 11:39 AM
 * This execution plan is a tree structure where logical operators
 * like AND/OR etc. have 2 children.
 * <p>
 * The children can themselves be nested trees.
 * </p>
 */
public class SimplePredicateEvaluationTree implements PredicateEvaluationTree {

    /**
     * This is the root predicate in the predicate tree.
     */
    private PredicateEvaluator rootPredicateEvaluator;

    @Override
    public <P extends PredicateEvaluator> void addEvaluator(P predicateEvaluator) {
        if (rootPredicateEvaluator == null) {
            this.rootPredicateEvaluator = predicateEvaluator;
        }
    }

    @SuppressWarnings("unchecked")
    public <V extends PredicateEvaluationTreeVisitor, R extends ResultStream> R accept(V visitor) {
        if (rootPredicateEvaluator instanceof RelationalPredicateEvaluator) {
            visitor.visit((RelationalPredicateEvaluator) rootPredicateEvaluator);
        } else if (rootPredicateEvaluator instanceof FilterPredicateEvaluator) {
            visitor.visit((FilterPredicateEvaluator) rootPredicateEvaluator);
        } else if (rootPredicateEvaluator instanceof BinaryPredicateEvaluator) {
            visitor.visit((BinaryPredicateEvaluator) rootPredicateEvaluator);
        } else if (rootPredicateEvaluator instanceof JoinPredicateEvaluator) {
            visitor.visit((JoinPredicateEvaluator) rootPredicateEvaluator);
        }
        return (R) visitor.getResult();
    }

    @Override
    public String toString() {
        return "SimplePredicateEvaluationTree{" +
                "rootPredicateEvaluator=" + rootPredicateEvaluator +
                '}';
    }
}
