package org.mem.store.query.exec;

import org.junit.Test;
import org.mem.store.persistence.descriptor.ColumnDescriptor;
import org.mem.store.persistence.descriptor.impl.DescriptorFactory;
import org.mem.store.persistence.service.invm.DataServiceFactory;
import org.mem.store.persistence.service.invm.InMemoryDataStoreService;
import org.mem.store.persistence.service.invm.InMemoryMetadataService;
import org.mem.store.query.exec.impl.invm.CostBasedQueryExecutionPlanGenerator;
import org.mem.store.query.model.BinaryOperator;
import org.mem.store.query.model.DataType;
import org.mem.store.query.model.Predicate;
import org.mem.store.query.model.Query;
import org.mem.store.query.model.impl.PredicateFactory;
import org.mem.store.query.model.impl.QueryImpl;
import org.mem.store.query.model.impl.SimpleQueryExpression;
import org.mem.store.query.model.impl.ValueExpression;

/**
 * Created by IntelliJ IDEA.
 * User: aathalye
 * Date: 16/12/13
 * Time: 3:44 PM
 * To change this template use File | Settings | File Templates.
 */
public class CostBasedPredicateTreeEvaluationTest extends RealPredicateTreeEvaluationTest {

    @Override
    public void setUp() {
        memoryQueryExecutionPlanGenerator = new CostBasedQueryExecutionPlanGenerator();

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

    @Override
    public void cleanup() {
        super.cleanup();
    }


    @Test
    public void evaluateCostBasedAndPredicateTree() {
        super.evaluateSimpleAndPredicateTree();
    }


    @Test
    public void evaluateCostBasedAndPredicateTreeWithGT() {
        super.evaluateSimpleAndPredicateTreeWithGT();
    }


    @Test
    public void evaluateCostBasedAndPredicateTreeWithLT() {
        super.evaluateSimpleAndPredicateTreeWithLT();
    }


    @Test
    public void evaluateCostBasedAndPredicateTreeWithGE() {
        super.evaluateSimpleAndPredicateTreeWithGE();
    }


    @Test
    public void evaluateCostBasedAndPredicateTreeWithLE() {
        super.evaluateSimpleAndPredicateTreeWithLE();
    }


    @Test
    public void evaluateCostBasedOrPredicateTree() {
        super.evaluateSimpleOrPredicateTree();
    }


    @Test
    public void evaluateCostBasedNestedAndPredicateTreePositive() {
        super.evaluateNestedAndPredicateTreePositive();
    }


    @Test
    public void evaluateCostBasedNestedAndPredicateTreeNegative() {
        super.evaluateNestedAndPredicateTreeNegative();
    }


    @Test
    public void evaluateCostBasedNestedOrPredicateTree() {
        super.evaluateNestedOrPredicateTree();
    }

    @Override
    protected Query<Predicate> buildTestPredicateForNestedAndNegative() {
        Query<Predicate> simpleAndQuery = buildTestPredicateForSimpleAnd();
        Predicate weekPredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(tables[0], "week"),
                        new ValueExpression<Integer>(2),
                        BinaryOperator.EQ);
        Predicate rootAndPredicate = PredicateFactory.createAndPredicate(weekPredicate, simpleAndQuery.getPredicate());
        Query<Predicate> query = new QueryImpl<Predicate>();
        query.setPredicate(rootAndPredicate);
        return query;
    }
}
