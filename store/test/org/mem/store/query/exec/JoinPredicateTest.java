package org.mem.store.query.exec;

import junit.framework.Assert;
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

/**
 * Created by IntelliJ IDEA.
 * User: aathalye
 * Date: 6/1/14
 * Time: 1:30 PM
 * To change this template use File | Settings | File Templates.
 */
public class JoinPredicateTest extends InMemoryPlanGeneratorTest {

    protected String[] tables = {"Employees", "Departments"};

    @Before
    public void setUp() {
        memoryQueryExecutionPlanGenerator = new InMemoryQueryExecutionPlanGenerator();

        ColumnDescriptor ageColumnDescriptor = DescriptorFactory.createColumnDescriptor("age", DataType.INTEGER, true);
        ColumnDescriptor deptColumnDescriptor = DescriptorFactory.createColumnDescriptor("dept", DataType.STRING, true);
        ColumnDescriptor nameColumnDescriptor = DescriptorFactory.createColumnDescriptor("name", DataType.STRING, true);

        InMemoryMetadataService metaService = DataServiceFactory.getInstance().getMetaDataService();
        InMemoryDataStoreService dataService = DataServiceFactory.getInstance().getDataStoreService();

        try {
            metaService.createTable(DescriptorFactory.createMemoryTableDescriptor(tables[0]));
            metaService.createIndex(tables[0], nameColumnDescriptor);
            metaService.createIndex(tables[0], ageColumnDescriptor);

            metaService.createTable(DescriptorFactory.createMemoryTableDescriptor(tables[1]));
            metaService.createIndex(tables[1], deptColumnDescriptor);
            metaService.createIndex(tables[1], ageColumnDescriptor);

            dataService.put(tables[0], buildTestTuple("aathalye", "age:32", "name:Aditya"));
            dataService.put(tables[0], buildTestTuple("jpatil", "age:23", "name:Jagruti"));
            dataService.put(tables[0], buildTestTuple("ntamhank", "age:32", "name:Nikhil"));
            dataService.put(tables[0], buildTestTuple("achavan", "age:43", "name:Abhay"));
            dataService.put(tables[0], buildTestTuple("bgokhale", "age:43", "name:Bala"));

            dataService.put(tables[1], buildTestTuple("FTBU_FOR_26", "age:26", "dept:FTBU"));
            dataService.put(tables[1], buildTestTuple("EPBU_FOR_23", "age:23", "dept:EPBU"));
            dataService.put(tables[1], buildTestTuple("FTBU_FOR_31", "age:31", "dept:FTBU"));
            dataService.put(tables[1], buildTestTuple("EPBU_FOR_43", "age:43", "dept:EPBU"));
            dataService.put(tables[1], buildTestTuple("FTBU_FOR_43", "age:43", "dept:FTBU"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    protected MemoryTuple buildTestTuple(String key, String... attrKeyValues) {
        MemoryKey memoryKey = new StringMemoryKey(key);
        MemoryTuple tuple1 = new MemoryTupleImpl(memoryKey);
        for (String keyValue : attrKeyValues) {
            String[] splits = keyValue.split(":");
            tuple1.setAttribute(splits[0], splits[1]);
        }
        return tuple1;
    }

    @Test
    public void evaluateSimpleJoinPredicateTreePositive() {
        InMemoryQueryExecutionPlan memoryQueryExecutionPlan = memoryQueryExecutionPlanGenerator.generateExecutionPlan(buildTestPredicateForSimpleJoinPositive());
        DefaultPredicateEvaluationTreeVisitor predicateEvaluationTreeVisitor = new DefaultPredicateEvaluationTreeVisitor();
        MutableResultStream mutableResultStream = memoryQueryExecutionPlan.execute(predicateEvaluationTreeVisitor);
        //No duplicate removal.
        Assert.assertEquals(5, mutableResultStream.getTuples().size());
    }

    @Test
    public void evaluateSimpleJoinPredicateTreeNegative() {
        InMemoryQueryExecutionPlan memoryQueryExecutionPlan = memoryQueryExecutionPlanGenerator.generateExecutionPlan(buildTestPredicateForSimpleJoinNegative());
        DefaultPredicateEvaluationTreeVisitor predicateEvaluationTreeVisitor = new DefaultPredicateEvaluationTreeVisitor();
        MutableResultStream mutableResultStream = memoryQueryExecutionPlan.execute(predicateEvaluationTreeVisitor);

        Assert.assertEquals(0, mutableResultStream.getTuples().size());
    }

    protected Query<Predicate> buildTestPredicateForSimpleJoinPositive() {
        Predicate eqJoinPredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(tables[0], "age"),
                        new SimpleQueryExpression(tables[1], "age"),
                        BinaryOperator.EQ);
        Query<Predicate> query = new QueryImpl<Predicate>();
        query.setPredicate(eqJoinPredicate);
        return query;
    }

    protected Query<Predicate> buildTestPredicateForSimpleJoinNegative() {
        Predicate eqJoinPredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(tables[0], "name"),
                        new SimpleQueryExpression(tables[1], "dept"),
                        BinaryOperator.EQ);
        Query<Predicate> query = new QueryImpl<Predicate>();
        query.setPredicate(eqJoinPredicate);
        return query;
    }
}
