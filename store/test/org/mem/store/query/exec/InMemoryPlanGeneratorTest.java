package org.mem.store.query.exec;


import org.junit.Before;
import org.junit.Test;
import org.mem.store.query.exec.impl.invm.InMemoryQueryExecutionPlanGenerator;
import org.mem.store.query.model.BinaryOperator;
import org.mem.store.query.model.LogicalPredicate;
import org.mem.store.query.model.Predicate;
import org.mem.store.query.model.Query;
import org.mem.store.query.model.impl.PredicateFactory;
import org.mem.store.query.model.impl.QueryImpl;
import org.mem.store.query.model.impl.ValueExpression;
import org.mem.store.query.model.impl.SimpleQueryExpression;

/**
 * Created by IntelliJ IDEA.
 * User: aathalye
 * Date: 29/11/13
 * Time: 3:08 PM
 * <p/>
 * Test class for plan generation.
 */
public class InMemoryPlanGeneratorTest {

    protected InMemoryQueryExecutionPlanGenerator memoryQueryExecutionPlanGenerator;

    protected String[] tables = {"MyTable"};

    @Before
    public void setUp() throws Exception {
        memoryQueryExecutionPlanGenerator = new InMemoryQueryExecutionPlanGenerator();
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

    protected Query<Predicate> buildTestPredicateForSimpleAnd() {
        Predicate eqPredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(tables[0], "dimLevel"),
                        new ValueExpression<String>("service"),
                        BinaryOperator.EQ);
        Predicate gtPredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(tables[0], "hitCount"),
                                                       new ValueExpression<Double>(5.0),
                                                       BinaryOperator.GT);
        Predicate andPredicate = PredicateFactory.createAndPredicate(eqPredicate, gtPredicate);
        Query<Predicate> query = new QueryImpl<Predicate>();
        query.setPredicate(andPredicate);
        return query;
    }

    protected Query<Predicate> buildTestPredicateForSimpleOr() {
        Predicate eqPredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(tables[0], "dimLevel"),
                                                       new ValueExpression<String>("service"),
                                                       BinaryOperator.EQ);
        Predicate gtPredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(tables[0], "hitCount"),
                                                       new ValueExpression<Double>(5.0),
                                                       BinaryOperator.GT);
        Predicate andPredicate = PredicateFactory.createOrPredicate(eqPredicate, gtPredicate);
        Query<Predicate> query = new QueryImpl<Predicate>();
        query.setPredicate(andPredicate);
        return query;
    }

    protected Query<Predicate> buildTestPredicateForNestedAnd() {
        Predicate eqPredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(tables[0], "dimLevel"),
                                                       new ValueExpression<String>("service"),
                                                       BinaryOperator.EQ);
        Predicate gtPredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(tables[0], "hitCount"),
                                                       new ValueExpression<Double>(5.0),
                                                       BinaryOperator.GT);
        Predicate ltPredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(tables[0], "hitCount"),
                                                       new ValueExpression<Double>(10.0),
                                                       BinaryOperator.LT);
        LogicalPredicate andPredicate = PredicateFactory.createAndPredicate(gtPredicate, ltPredicate);
        Predicate rootAndPredicate = PredicateFactory.createAndPredicate(eqPredicate, andPredicate);
        Query<Predicate> query = new QueryImpl<Predicate>();
        query.setPredicate(rootAndPredicate);
        return query;
    }

    protected Query<Predicate> buildTestPredicateForNestedOr() {
        Predicate eqPredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(tables[0], "dimLevel"),
                                                       new ValueExpression<String>("service"),
                                                       BinaryOperator.EQ);
        Predicate gtPredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(tables[0], "hitCount"),
                                                       new ValueExpression<Double>(5.0),
                                                       BinaryOperator.GT);
        Predicate ltPredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(tables[0], "hitCount"),
                                                       new ValueExpression<Double>(10.0),
                                                       BinaryOperator.LT);
        LogicalPredicate andPredicate = PredicateFactory.createAndPredicate(gtPredicate, ltPredicate);
        Predicate rootOrPredicate = PredicateFactory.createOrPredicate(eqPredicate, andPredicate);
        Query<Predicate> query = new QueryImpl<Predicate>();
        query.setPredicate(rootOrPredicate);
        return query;
    }

    protected Query<Predicate> buildTestPredicateForOrWithinAnd() {
        Predicate eqPredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(tables[0], "dimLevel"),
                                                       new ValueExpression<String>("service"),
                                                       BinaryOperator.EQ);
        Predicate gtPredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(tables[0], "hitCount"),
                                                       new ValueExpression<Double>(5.0),
                                                       BinaryOperator.GT);
        Predicate ltPredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(tables[0], "hitCount"),
                                                       new ValueExpression<Double>(10.0),
                                                       BinaryOperator.LT);
        LogicalPredicate orPredicate = PredicateFactory.createOrPredicate(gtPredicate, ltPredicate);
        Predicate rootAndPredicate =  PredicateFactory.createAndPredicate(orPredicate, eqPredicate);
        Query<Predicate> query = new QueryImpl<Predicate>();
        query.setPredicate(rootAndPredicate);
        return query;
    }
}
