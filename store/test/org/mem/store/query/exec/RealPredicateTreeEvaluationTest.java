package org.mem.store.query.exec;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mem.store.persistence.descriptor.ColumnDescriptor;
import org.mem.store.persistence.descriptor.impl.DescriptorFactory;
import org.mem.store.persistence.model.MemoryKey;
import org.mem.store.persistence.model.MemoryTuple;
import org.mem.store.persistence.model.invm.impl.MemoryTupleImpl;
import org.mem.store.persistence.model.invm.impl.StringMemoryKey;
import org.mem.store.persistence.service.invm.DataServiceFactory;
import org.mem.store.persistence.service.invm.InMemoryDataStoreService;
import org.mem.store.persistence.service.invm.InMemoryMetadataService;
import org.mem.store.query.exec.impl.invm.DefaultPredicateEvaluationTreeVisitor;
import org.mem.store.query.exec.impl.invm.InMemoryQueryExecutionPlan;
import org.mem.store.query.exec.impl.invm.InMemoryQueryExecutionPlanGenerator;
import org.mem.store.query.model.BinaryOperator;
import org.mem.store.query.model.DataType;
import org.mem.store.query.model.Predicate;
import org.mem.store.query.model.Query;
import org.mem.store.query.model.impl.MutableResultStream;
import org.mem.store.query.model.impl.PredicateFactory;
import org.mem.store.query.model.impl.QueryImpl;
import org.mem.store.query.model.impl.SimpleQueryExpression;
import org.mem.store.query.model.impl.ValueExpression;

/**
 * Created by IntelliJ IDEA.
 * User: aathalye
 * Date: 11/12/13
 * Time: 2:37 PM
 * To change this template use File | Settings | File Templates.
 */
public class RealPredicateTreeEvaluationTest extends InMemoryPlanGeneratorTest {

    protected MemoryTuple buildTestTuple(String key, Object... attrValues) {
        MemoryKey memoryKey = new StringMemoryKey(key);
        MemoryTuple tuple1 = new MemoryTupleImpl(memoryKey);
        tuple1.setAttribute("environment", attrValues[0]);
        tuple1.setAttribute("service", attrValues[1]);
        tuple1.setAttribute("week", attrValues[2]);
        return tuple1;
    }

    @Before
    public void setUp() {
        memoryQueryExecutionPlanGenerator = new InMemoryQueryExecutionPlanGenerator();

        ColumnDescriptor envColumnDescriptor = DescriptorFactory.createColumnDescriptor("environment", DataType.STRING, true);
        ColumnDescriptor serviceColumnDescriptor = DescriptorFactory.createColumnDescriptor("service", DataType.STRING, true);
        ColumnDescriptor weekColumnDescriptor = DescriptorFactory.createColumnDescriptor("week", DataType.INTEGER, true);

        InMemoryMetadataService metaDataService = DataServiceFactory.getInstance().getMetaDataService();
        InMemoryDataStoreService dataService = DataServiceFactory.getInstance().getDataStoreService();

        try {
            metaDataService.createTable(DescriptorFactory.createMemoryTableDescriptor("MyTable"));
            metaDataService.createIndex("MyTable", envColumnDescriptor);
            metaDataService.createIndex("MyTable", serviceColumnDescriptor);
            metaDataService.createIndex("MyTable", weekColumnDescriptor);

            dataService.put("MyTable", buildTestTuple("/env1/service1/3", "env1", "service1", 3));
            dataService.put("MyTable", buildTestTuple("/env1/service2/2", "env1", "service2", 2));
            dataService.put("MyTable", buildTestTuple("/env2/service1/4", "env2", "service1", 4));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void cleanup() {
        DataServiceFactory.getInstance().getDataStoreService().clear("MyTable");
        DataServiceFactory.getInstance().getMetaDataService().deleteTable("MyTable");
    }

    @Test
    /**
     * Query : env = "env1" & service = "service1"
     */
    public void evaluateSimpleAndPredicateTree() {
        InMemoryQueryExecutionPlan memoryQueryExecutionPlan = memoryQueryExecutionPlanGenerator.generateExecutionPlan(buildTestPredicateForSimpleAnd());
        DefaultPredicateEvaluationTreeVisitor predicateEvaluationTreeVisitor = new DefaultPredicateEvaluationTreeVisitor();
        MutableResultStream mutableResultStream = memoryQueryExecutionPlan.execute(predicateEvaluationTreeVisitor);
        Assert.assertEquals(1, mutableResultStream.getTuples().size());
    }

    @Test
    /**
     * Query : service = "service1" & week > 2
     */
    public void evaluateSimpleAndPredicateTreeWithGT() {
        InMemoryQueryExecutionPlan memoryQueryExecutionPlan = memoryQueryExecutionPlanGenerator.generateExecutionPlan(buildTestPredicateForSimpleAndWithGT());
        DefaultPredicateEvaluationTreeVisitor predicateEvaluationTreeVisitor = new DefaultPredicateEvaluationTreeVisitor();
        MutableResultStream mutableResultStream = memoryQueryExecutionPlan.execute(predicateEvaluationTreeVisitor);
        Assert.assertEquals(2, mutableResultStream.getTuples().size());
    }

    @Test
    /**
     * Query : service = "service1" & week < 2
     */
    public void evaluateSimpleAndPredicateTreeWithLT() {
        InMemoryQueryExecutionPlan memoryQueryExecutionPlan = memoryQueryExecutionPlanGenerator.generateExecutionPlan(buildTestPredicateForSimpleAndWithLT());
        DefaultPredicateEvaluationTreeVisitor predicateEvaluationTreeVisitor = new DefaultPredicateEvaluationTreeVisitor();
        MutableResultStream mutableResultStream = memoryQueryExecutionPlan.execute(predicateEvaluationTreeVisitor);
        Assert.assertEquals(0, mutableResultStream.getTuples().size());
    }

    @Test
    /**
     * Query : service = "service1" & week >= 4
     */
    public void evaluateSimpleAndPredicateTreeWithGE() {
        InMemoryQueryExecutionPlan memoryQueryExecutionPlan = memoryQueryExecutionPlanGenerator.generateExecutionPlan(buildTestPredicateForSimpleAndWithGE());
        DefaultPredicateEvaluationTreeVisitor predicateEvaluationTreeVisitor = new DefaultPredicateEvaluationTreeVisitor();
        MutableResultStream mutableResultStream = memoryQueryExecutionPlan.execute(predicateEvaluationTreeVisitor);
        Assert.assertEquals(1, mutableResultStream.getTuples().size());
    }

    @Test
    /**
     * Query : service = "service1" & week != 4
     */
    public void evaluateSimpleAndPredicateTreeWithNE() {
        InMemoryQueryExecutionPlan memoryQueryExecutionPlan = memoryQueryExecutionPlanGenerator.generateExecutionPlan(buildTestPredicateForSimpleAndWithNE());
        DefaultPredicateEvaluationTreeVisitor predicateEvaluationTreeVisitor = new DefaultPredicateEvaluationTreeVisitor();
        MutableResultStream mutableResultStream = memoryQueryExecutionPlan.execute(predicateEvaluationTreeVisitor);
        Assert.assertEquals(1, mutableResultStream.getTuples().size());
    }

    @Test
    /**
     * Query : env = "env1" & week <= 2
     */
    public void evaluateSimpleAndPredicateTreeWithLE() {
        InMemoryQueryExecutionPlan memoryQueryExecutionPlan = memoryQueryExecutionPlanGenerator.generateExecutionPlan(buildTestPredicateForSimpleAndWithLE());
        DefaultPredicateEvaluationTreeVisitor predicateEvaluationTreeVisitor = new DefaultPredicateEvaluationTreeVisitor();
        MutableResultStream mutableResultStream = memoryQueryExecutionPlan.execute(predicateEvaluationTreeVisitor);
        Assert.assertEquals(1, mutableResultStream.getTuples().size());
    }

    @Test
    /**
     * Query : env = "env1" || service = "service1"
     */
    public void evaluateSimpleOrPredicateTree() {
        InMemoryQueryExecutionPlan memoryQueryExecutionPlan = memoryQueryExecutionPlanGenerator.generateExecutionPlan(buildTestPredicateForSimpleOr());
        DefaultPredicateEvaluationTreeVisitor predicateEvaluationTreeVisitor = new DefaultPredicateEvaluationTreeVisitor();
        MutableResultStream mutableResultStream = memoryQueryExecutionPlan.execute(predicateEvaluationTreeVisitor);
        //We get 4 results after with duplicates.
        Assert.assertEquals(4, mutableResultStream.getTuples().size());
    }

    @Test
    /**
     * Query : env = "env1" & service = "service1" & week = 3
     */
    public void evaluateNestedAndPredicateTreePositive() {
        InMemoryQueryExecutionPlan memoryQueryExecutionPlan = memoryQueryExecutionPlanGenerator.generateExecutionPlan(buildTestPredicateForNestedAndPositive());
        DefaultPredicateEvaluationTreeVisitor predicateEvaluationTreeVisitor = new DefaultPredicateEvaluationTreeVisitor();
        MutableResultStream mutableResultStream = memoryQueryExecutionPlan.execute(predicateEvaluationTreeVisitor);
        //We should get 1 result
        Assert.assertEquals(1, mutableResultStream.getTuples().size());
    }

    @Test
    /**
     * Query : env = "env1" & service = "service1" & week = 2
     */
    public void evaluateNestedAndPredicateTreeNegative() {
        InMemoryQueryExecutionPlan memoryQueryExecutionPlan = memoryQueryExecutionPlanGenerator.generateExecutionPlan(buildTestPredicateForNestedAndNegative());
        DefaultPredicateEvaluationTreeVisitor predicateEvaluationTreeVisitor = new DefaultPredicateEvaluationTreeVisitor();
        MutableResultStream mutableResultStream = memoryQueryExecutionPlan.execute(predicateEvaluationTreeVisitor);
        //We should get 0 results
        Assert.assertEquals(0, mutableResultStream.getTuples().size());
    }

    @Test
    /**
     * Query : env = "env1" || service = "service1" || week = 3
     */
    public void evaluateNestedOrPredicateTree() {
        InMemoryQueryExecutionPlan memoryQueryExecutionPlan = memoryQueryExecutionPlanGenerator.generateExecutionPlan(buildTestPredicateForNestedOr());
        DefaultPredicateEvaluationTreeVisitor predicateEvaluationTreeVisitor = new DefaultPredicateEvaluationTreeVisitor();
        MutableResultStream mutableResultStream = memoryQueryExecutionPlan.execute(predicateEvaluationTreeVisitor);
        //We should get 5 results
        Assert.assertEquals(5, mutableResultStream.getTuples().size());
    }

    protected Query<Predicate> buildTestPredicateForSimpleAnd() {
        Predicate servicePredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(tables[0], "service"),
                        new ValueExpression<String>("service1"),
                        BinaryOperator.EQ);
        Predicate envPredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(tables[0], "environment"),
                        new ValueExpression<String>("env1"),
                        BinaryOperator.EQ);
        Predicate rootAndPredicate = PredicateFactory.createAndPredicate(servicePredicate, envPredicate);
        Query<Predicate> query = new QueryImpl<Predicate>();
        query.setPredicate(rootAndPredicate);
        return query;
    }

    @Override
    protected Query<Predicate> buildTestPredicateForSimpleOr() {
        Predicate servicePredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(tables[0], "service"),
                        new ValueExpression<String>("service1"),
                        BinaryOperator.EQ);
        Predicate envPredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(tables[0], "environment"),
                        new ValueExpression<String>("env1"),
                        BinaryOperator.EQ);
        Predicate orPredicate = PredicateFactory.createOrPredicate(servicePredicate, envPredicate);
        Query<Predicate> query = new QueryImpl<Predicate>();
        query.setPredicate(orPredicate);
        return query;
    }

    protected Query<Predicate> buildTestPredicateForNestedAndPositive() {
        Query<Predicate> simpleAndQuery = buildTestPredicateForSimpleAnd();
        Predicate weekPredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(tables[0], "week"),
                        new ValueExpression<Integer>(3),
                        BinaryOperator.EQ);
        Predicate rootAndPredicate = PredicateFactory.createAndPredicate(simpleAndQuery.getPredicate(), weekPredicate);
        Query<Predicate> query = new QueryImpl<Predicate>();
        query.setPredicate(rootAndPredicate);
        return query;
    }

    protected Query<Predicate> buildTestPredicateForNestedAndNegative() {
        Query<Predicate> simpleAndQuery = buildTestPredicateForSimpleAnd();
        Predicate weekPredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(tables[0], "week"),
                        new ValueExpression<Integer>(2),
                        BinaryOperator.EQ);
        Predicate rootAndPredicate = PredicateFactory.createAndPredicate(simpleAndQuery.getPredicate(), weekPredicate);
        Query<Predicate> query = new QueryImpl<Predicate>();
        query.setPredicate(rootAndPredicate);
        return query;
    }

    @Override
    protected Query<Predicate> buildTestPredicateForNestedOr() {
        Query<Predicate> simpleOrQuery = buildTestPredicateForSimpleOr();
        Predicate weekPredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(tables[0], "week"),
                        new ValueExpression<Integer>(3),
                        BinaryOperator.EQ);
        Predicate rootOrPredicate = PredicateFactory.createOrPredicate(simpleOrQuery.getPredicate(), weekPredicate);
        Query<Predicate> query = new QueryImpl<Predicate>();
        query.setPredicate(rootOrPredicate);
        return query;
    }

    protected Query<Predicate> buildTestPredicateForSimpleAndWithGT() {
        Predicate servicePredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(tables[0], "service"),
                        new ValueExpression<String>("service1"),
                        BinaryOperator.EQ);
        Predicate weekPredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(tables[0], "week"),
                        new ValueExpression<Integer>(2),
                        BinaryOperator.GT);
        Predicate rootAndPredicate = PredicateFactory.createAndPredicate(servicePredicate, weekPredicate);
        Query<Predicate> query = new QueryImpl<Predicate>();
        query.setPredicate(rootAndPredicate);
        return query;
    }

    protected Query<Predicate> buildTestPredicateForSimpleAndWithLT() {
        Predicate servicePredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(tables[0], "service"),
                        new ValueExpression<String>("service1"),
                        BinaryOperator.EQ);
        Predicate weekPredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(tables[0], "week"),
                        new ValueExpression<Integer>(2),
                        BinaryOperator.LT);
        Predicate rootAndPredicate = PredicateFactory.createAndPredicate(servicePredicate, weekPredicate);
        Query<Predicate> query = new QueryImpl<Predicate>();
        query.setPredicate(rootAndPredicate);
        return query;
    }

    protected Query<Predicate> buildTestPredicateForSimpleAndWithGE() {
        Predicate servicePredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(tables[0], "service"),
                        new ValueExpression<String>("service1"),
                        BinaryOperator.EQ);
        Predicate weekPredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(tables[0], "week"),
                        new ValueExpression<Integer>(4),
                        BinaryOperator.LE);
        Predicate rootAndPredicate = PredicateFactory.createAndPredicate(servicePredicate, weekPredicate);
        Query<Predicate> query = new QueryImpl<Predicate>();
        query.setPredicate(rootAndPredicate);
        return query;
    }

    protected Query<Predicate> buildTestPredicateForSimpleAndWithNE() {
        Predicate servicePredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(tables[0], "service"),
                        new ValueExpression<String>("service1"),
                        BinaryOperator.EQ);
        Predicate weekPredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(tables[0], "week"),
                        new ValueExpression<Integer>(4),
                        BinaryOperator.NOTEQ);
        Predicate rootAndPredicate = PredicateFactory.createAndPredicate(servicePredicate, weekPredicate);
        Query<Predicate> query = new QueryImpl<Predicate>();
        query.setPredicate(rootAndPredicate);
        return query;
    }

    protected Query<Predicate> buildTestPredicateForSimpleAndWithLE() {
        Predicate envPredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(tables[0], "environment"),
                        new ValueExpression<String>("env1"),
                        BinaryOperator.EQ);
        Predicate weekPredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(tables[0], "week"),
                        new ValueExpression<Integer>(2),
                        BinaryOperator.LE);
        Predicate rootAndPredicate = PredicateFactory.createAndPredicate(envPredicate, weekPredicate);
        Query<Predicate> query = new QueryImpl<Predicate>();
        query.setPredicate(rootAndPredicate);
        return query;
    }
}
