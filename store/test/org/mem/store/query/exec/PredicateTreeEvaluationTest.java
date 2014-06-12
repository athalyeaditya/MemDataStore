package org.mem.store.query.exec;

import junit.framework.Assert;
import org.junit.Test;
import org.mem.store.query.exec.impl.invm.DefaultPredicateEvaluationTreeVisitor;
import org.mem.store.query.exec.impl.invm.InMemoryQueryExecutionPlan;
import org.mem.store.query.model.impl.MutableResultStream;

/**
 * Created by IntelliJ IDEA.
 * User: aathalye
 * Date: 4/12/13
 * Time: 11:28 AM
 * To change this template use File | Settings | File Templates.
 */
public class PredicateTreeEvaluationTest extends InMemoryPlanGeneratorTest {

    @Test
    public void evaluateSimpleAndPredicateTree() {
        InMemoryQueryExecutionPlan memoryQueryExecutionPlan = memoryQueryExecutionPlanGenerator.generateExecutionPlan(buildTestPredicateForSimpleAnd());
        DefaultPredicateEvaluationTreeVisitor predicateEvaluationTreeVisitor = new DefaultPredicateEvaluationTreeVisitor();
        MutableResultStream mutableResultStream = memoryQueryExecutionPlan.execute(predicateEvaluationTreeVisitor);
        Assert.assertEquals(1, mutableResultStream.getTuples().size());
    }

    @Test
    public void evaluateSimpleOrPredicateTree() {
        InMemoryQueryExecutionPlan memoryQueryExecutionPlan = memoryQueryExecutionPlanGenerator.generateExecutionPlan( buildTestPredicateForSimpleOr());
        DefaultPredicateEvaluationTreeVisitor predicateEvaluationTreeVisitor = new DefaultPredicateEvaluationTreeVisitor();
        MutableResultStream mutableResultStream = memoryQueryExecutionPlan.execute(predicateEvaluationTreeVisitor);
        Assert.assertEquals(2, mutableResultStream.getTuples().size());
    }

    @Test
    public void evaluateNestedPredicateTreeWithAnd() {
        InMemoryQueryExecutionPlan memoryQueryExecutionPlan = memoryQueryExecutionPlanGenerator.generateExecutionPlan( buildTestPredicateForNestedAnd());
        DefaultPredicateEvaluationTreeVisitor predicateEvaluationTreeVisitor = new DefaultPredicateEvaluationTreeVisitor();
        MutableResultStream mutableResultStream = memoryQueryExecutionPlan.execute(predicateEvaluationTreeVisitor);
        Assert.assertEquals(1, mutableResultStream.getTuples().size());
    }

    @Test
    public void evaluateNestedPredicateTreeWithOr() {
        InMemoryQueryExecutionPlan memoryQueryExecutionPlan = memoryQueryExecutionPlanGenerator.generateExecutionPlan( buildTestPredicateForNestedOr());
        DefaultPredicateEvaluationTreeVisitor predicateEvaluationTreeVisitor = new DefaultPredicateEvaluationTreeVisitor();
        MutableResultStream mutableResultStream = memoryQueryExecutionPlan.execute(predicateEvaluationTreeVisitor);
        //We are expecting or
        Assert.assertEquals(2, mutableResultStream.getTuples().size());
    }
}
