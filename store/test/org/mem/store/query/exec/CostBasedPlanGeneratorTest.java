package org.mem.store.query.exec;

import org.junit.Before;
import org.junit.Test;
import org.mem.store.query.exec.impl.invm.CostBasedQueryExecutionPlanGenerator;

/**
 * Created by IntelliJ IDEA.
 * User: aathalye
 * Date: 16/12/13
 * Time: 11:32 AM
 * To change this template use File | Settings | File Templates.
 */
public class CostBasedPlanGeneratorTest extends InMemoryPlanGeneratorTest {

    @Override
    @Before
    public void setUp() throws Exception {
        memoryQueryExecutionPlanGenerator = new CostBasedQueryExecutionPlanGenerator();
    }

    @Test
    public void generatePredicateTreeForSingleLevelPredicate() {
        //TODO add asserts
        memoryQueryExecutionPlanGenerator.generateExecutionPlan(buildTestPredicateForSimpleAnd());
    }

    @Test
    public void generatePredicateTreeForNestedAndPredicate() {
        //TODO add asserts
        memoryQueryExecutionPlanGenerator.generateExecutionPlan(buildTestPredicateForNestedAnd());
    }

    @Test
    public void generatePredicateTreeForNestedOrPredicate() {
        //TODO add asserts
        memoryQueryExecutionPlanGenerator.generateExecutionPlan(buildTestPredicateForNestedOr());
    }

    @Test
    public void generatePredicateTreeForOrWithinAnd() {
        //TODO add asserts
        memoryQueryExecutionPlanGenerator.generateExecutionPlan(buildTestPredicateForOrWithinAnd());
    }
}
