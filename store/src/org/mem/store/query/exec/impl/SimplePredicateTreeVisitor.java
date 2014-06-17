package org.mem.store.query.exec.impl;

import org.mem.store.query.exec.BinaryPredicateEvaluator;
import org.mem.store.query.exec.PredicateEvaluator;
import org.mem.store.query.exec.PredicateTreeVisitor;
import org.mem.store.query.exec.RelationalPredicateEvaluator;
import org.mem.store.query.model.LogicalPredicate;
import org.mem.store.query.model.Predicate;
import org.mem.store.query.model.RelationalPredicate;
import org.mem.store.query.model.impl.AndPredicate;
import org.mem.store.query.model.impl.OrPredicate;

import java.util.Stack;

/**
 * Created by IntelliJ IDEA.
 * User: aathalye
 * Date: 17/12/13
 * Time: 4:20 PM
 *
 * Simple predicate visitor.
 */
public class SimplePredicateTreeVisitor implements PredicateTreeVisitor {

    /**
     * Used for construction internally.
     */
    private Stack<PredicateEvaluator> predicateEvaluatorStack = new Stack<PredicateEvaluator>();

    @Override
    public void visit(AndPredicate andPredicate) {
        visitLogicalPredicate(andPredicate);
    }

    @Override
    public void visit(OrPredicate orPredicate) {
        visitLogicalPredicate(orPredicate);
    }

    /**
     * Visit logical predicate in pre order fashion.
     *
     */
    private <L extends LogicalPredicate> void visitLogicalPredicate(L logicalPredicate) {
        Predicate[] childPredicates = logicalPredicate.getChildPredicates();
        //Visit root
        processLogicalPredicate(logicalPredicate);
        //There will be only 2 visit pre order
        visitChild(childPredicates[0]);
        //Visit right
        visitChild(childPredicates[1]);
    }

    /**
     *
     * Visit relational predicate and create evaluator for it.
     */
    @Override
    public void visit(RelationalPredicate relationalPredicate) {
        PredicateEvaluator predicateEvaluator = PredicateEvaluatorFactory.createPredicateEvaluator(relationalPredicate);

        processPredicateEvaluator(predicateEvaluator);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <P extends PredicateEvaluator> P getResult() {
        return (P) pop();
    }

    protected <P extends Predicate> void visitChild(P childPredicate) {
        if (childPredicate != null) {
            if (childPredicate instanceof RelationalPredicate) {
                visit((RelationalPredicate) childPredicate);
            } else if (childPredicate instanceof AndPredicate) {
                visit((AndPredicate) childPredicate);
            } else if (childPredicate instanceof OrPredicate) {
                visit((OrPredicate) childPredicate);
            }
        }
    }

    /**
     * Process logical predicate and create evaluator for it.
     *
     */
    @SuppressWarnings("unchecked")
    protected <L extends LogicalPredicate> void processLogicalPredicate(L logicalPredicate) {
        PredicateEvaluator predicateEvaluator = null;
        if (logicalPredicate instanceof AndPredicate) {
            predicateEvaluator = new SimpleFilterPredicateEvaluator(logicalPredicate);
        } else if (logicalPredicate instanceof OrPredicate) {
            predicateEvaluator = new SimpleBinaryPredicateEvaluator<PredicateEvaluator>(logicalPredicate);
        }
        processPredicateEvaluator(predicateEvaluator);
    }

    /**
     *
     * Process a child evaluator and possibly add it to its parent.
     */
    protected <P extends PredicateEvaluator> void processPredicateEvaluator(P predicateEvaluator) {
        //Get topmost and add as child
        PredicateEvaluator topEvaluator = peek();

        if (topEvaluator instanceof BinaryPredicateEvaluator) {
            new BinaryPredicateEvaluatorAdapter((BinaryPredicateEvaluator) topEvaluator, predicateEvaluator)
                    .handleTopBinaryPredicateEvaluator();
        } else {
            //Push it on stack
            push(predicateEvaluator);
        }
    }


    private <P extends PredicateEvaluator> void push(P predicateEvaluator) {
        predicateEvaluatorStack.push(predicateEvaluator);
    }

    @SuppressWarnings("unchecked")
    private <P extends PredicateEvaluator> P peek() {
        return (predicateEvaluatorStack.isEmpty()) ? null : (P) predicateEvaluatorStack.peek();
    }

    private int size() {
        return predicateEvaluatorStack.size();
    }

    @SuppressWarnings("unchecked")
    private <P extends PredicateEvaluator> P pop() {
        return (P) predicateEvaluatorStack.pop();
    }

    /**
     * Handle case when top is binary predicate evaluator
     */
    private class BinaryPredicateEvaluatorAdapter {

        private BinaryPredicateEvaluator<?> topPredicateEvaluator;

        private PredicateEvaluator childPredicateEvaluator;

        BinaryPredicateEvaluatorAdapter(BinaryPredicateEvaluator<?> topPredicateEvaluator, PredicateEvaluator childPredicateEvaluator) {
            this.topPredicateEvaluator = topPredicateEvaluator;
            this.childPredicateEvaluator = childPredicateEvaluator;
        }

        private void handleTopBinaryPredicateEvaluator() {
            topPredicateEvaluator.addChildPredicateEvaluator(childPredicateEvaluator);
            //If not added pop the root as it means it is full. Also do not pop if it is last element
            if (topPredicateEvaluator.isFull() && size() > 1) {
                pop();
            }
            if (!(childPredicateEvaluator instanceof RelationalPredicateEvaluator)) {
                //We want both chained and binary to be pushed in this case
                push(childPredicateEvaluator);
            }
        }
    }
}
