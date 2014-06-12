package org.mem.store.persistence;


import junit.framework.Assert;
import org.junit.Test;
import org.mem.store.persistence.model.MemoryTuple;
import org.mem.store.persistence.model.TimeUtils;
import org.mem.store.query.exec.QueryExecutor;
import org.mem.store.query.exec.impl.invm.InMemoryQueryExecutor;
import org.mem.store.query.model.BinaryOperator;
import org.mem.store.query.model.Predicate;
import org.mem.store.query.model.Query;
import org.mem.store.query.model.QueryResultSet;
import org.mem.store.query.model.impl.PredicateFactory;
import org.mem.store.query.model.impl.QueryImpl;
import org.mem.store.query.model.impl.ValueExpression;
import org.mem.store.query.model.impl.SimpleQueryExpression;

import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Application Trends queries with no optimization.
 */
public class AppTrendsTableTest extends SPMTablesQueryTest {

    @Test
    public void evaluateAppsWeeklySummaryQuery() {
        Query<Predicate> queryModel = buildAppsWeeklySummaryQuery();
        executeAndAssertQuery(queryModel, 5, "ENVIRONMENT", "APPLICATION_NAME", "SERVICE_TYPE", "HITCOUNT", "SUCCESSCOUNT", "FAULTCOUNT", "AVGRESPONSETIME", "WEEKS");
    }

     /*
     * query =  "SELECT ENVIRONMENT, APPLICATION_NAME, HITCOUNT, SUCCESSCOUNT, FAULTCOUT, AVGRESPONSETIME
     * 			FROM METRIC_APPLTRENDS
     * 			WHERE DIM_LEVEL_NAME = \"weeks\" AND SERVICE_TYPE = \"Service\"";
     */
    protected Query<Predicate> buildAppsWeeklySummaryQuery() {
        String query = "SELECT ENVIRONMENT, APPLICATION_NAME, HITCOUNT, SUCCESSCOUNT, FAULTCOUT, AVGRESPONSETIME FROM METRIC_APPLTRENDS WHERE ";
        Predicate dimNamePredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(applTrendsTable, "DIM_LEVEL_NAME"),
                        new ValueExpression<String>("weeks"),
                        BinaryOperator.EQ);
        long thisWeek = TimeUtils.getThisWeek(systemTime);
        Predicate weekPredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(applTrendsTable, "WEEKS"),
                                                       new ValueExpression<Long>(thisWeek),
                                                       BinaryOperator.EQ);
        Predicate serviceTypePredicate =
                PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(applTrendsTable, "SERVICE_TYPE"), new ValueExpression<String>("Service"), BinaryOperator.EQ);
        Predicate andPredicate = PredicateFactory.createAndPredicate(dimNamePredicate, serviceTypePredicate);
        Predicate compositePredicate = PredicateFactory.createAndPredicate(andPredicate, weekPredicate);
        Query<Predicate> queryModel = new QueryImpl<Predicate>();
        queryModel.setPredicate(compositePredicate);

        return queryModel;
    }

    @Test
    public void evaluateAppsResponseTimeByWeekQuery() {
        Query<Predicate> queryModel = buildAppsResponseTimeByWeekQuery();
        executeAndAssertQuery(queryModel, 14, "AVGRESPONSETIME", "WEEKS", "ENVIRONMENT", "APPLICATION_NAME", "SERVICE_TYPE");
    }

     /*
     * query =  SELECT AVGRESPONSETIME, DAYS
     * 			FROM METRIC_APPLTRENDS
     * 			WHERE ENVIRONMENT = "environment_1" AND APPLICATION_NAME = "wealthApp_1" AND DIM_LEVEL_NAME = "days" AND WEEKS >= last_week AND weeks <= this_week
     * 			AND SERVICE_TYPE = "Service";
     */
    protected Query<Predicate> buildAppsResponseTimeByWeekQuery() {

        String query = "SELECT AVGRESPONSETIME, DAYS FROM METRIC_APPLTRENDS WHERE ";

        long currWeekDimValue =  TimeUtils.getThisWeek(systemTime);
        System.out.println("this week: "+ new Date(currWeekDimValue));
        Predicate weekLEThisWeek = PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(applTrendsTable, "WEEKS"), new ValueExpression<Long>(currWeekDimValue), BinaryOperator.LE);

        long prevWeekDimValue = currWeekDimValue - TimeUnit.MILLISECONDS.convert(7, TimeUnit.DAYS);
        System.out.println("prev week: "+ new Date(prevWeekDimValue));
        Predicate weekGEPrevWeek = PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(applTrendsTable, "WEEKS"), new ValueExpression<Long>(prevWeekDimValue), BinaryOperator.GE);

        Predicate andWeeksPredicate = PredicateFactory.createAndPredicate(weekGEPrevWeek, weekLEThisWeek);

        Predicate dimNamePredicate = PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(applTrendsTable, "DIM_LEVEL_NAME"), new ValueExpression<String>("days"), BinaryOperator.EQ);
        Predicate environmentPredicate = PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(applTrendsTable, "ENVIRONMENT"), new ValueExpression<String>("environment_0"), BinaryOperator.EQ);

        Predicate andLevelEnvPredicate = PredicateFactory.createAndPredicate(dimNamePredicate, environmentPredicate);

        Predicate applicationPredicate = PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(applTrendsTable, "APPLICATION_NAME"), new ValueExpression<String>("wealthApp_0"), BinaryOperator.EQ);
        Predicate serviceTypePredicate = PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(applTrendsTable, "SERVICE_TYPE"), new ValueExpression<String>("Service"), BinaryOperator.EQ);

        Predicate andAppSvcPredicate = PredicateFactory.createAndPredicate(applicationPredicate, serviceTypePredicate);

        Predicate compositePredicate = PredicateFactory.createAndPredicate(andAppSvcPredicate, andLevelEnvPredicate);

        Predicate finalCompositePredicate = PredicateFactory.createAndPredicate(andWeeksPredicate, compositePredicate);

        Query<Predicate> queryModel = new QueryImpl<Predicate>();
        queryModel.setPredicate(finalCompositePredicate);

        return queryModel;
    }


    @Test
    public void evaluateAppsResponseTimeByDayQuery() {
        Query<Predicate> queryModel = buildAppsResponseTimeByDayQuery();
        executeAndAssertQuery(queryModel, 48, "AVGRESPONSETIME", "HOURS", "ENVIRONMENT", "APPLICATION_NAME", "SERVICE_TYPE");
    }

     /*
     * query =  SELECT AVGRESPONSETIME, DAYS
     * 			FROM METRIC_APPLTRENDS
     * 			WHERE ENVIRONMENT = "environment_1" AND APPLICATION_NAME = "wealthApp_1" AND DIM_LEVEL_NAME = "hours" AND DAYS >= last_day AND DAYS <= this_day
     * 			AND SERVICE_TYPE = "Service";
     */

    protected Query<Predicate> buildAppsResponseTimeByDayQuery() {
        String query = "SELECT AVGRESPONSETIME, DAYS FROM METRIC_APPLTRENDS WHERE ";

        long currDayDimValue =  TimeUtils.getThisDay(systemTime);
        System.out.println("this day: "+ new Date(currDayDimValue));
        Predicate weekLEThisDay = PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(applTrendsTable, "DAYS"), new ValueExpression<Long>(currDayDimValue), BinaryOperator.LE);

        long prevDayDimValue = currDayDimValue - TimeUnit.MILLISECONDS.convert(24, TimeUnit.HOURS);
        System.out.println("prev day: "+ new Date(prevDayDimValue));
        Predicate weekGEPrevDay = PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(applTrendsTable, "DAYS"), new ValueExpression<Long>(prevDayDimValue), BinaryOperator.GE);

        Predicate andDaysPredicate = PredicateFactory.createAndPredicate(weekGEPrevDay, weekLEThisDay);

        Predicate dimNamePredicate = PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(applTrendsTable, "DIM_LEVEL_NAME"), new ValueExpression<String>("hours"), BinaryOperator.EQ);
        Predicate environmentPredicate = PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(applTrendsTable, "ENVIRONMENT"), new ValueExpression<String>("environment_0"), BinaryOperator.EQ);

        Predicate andLevelEnvPredicate = PredicateFactory.createAndPredicate(dimNamePredicate, environmentPredicate);

        Predicate applicationPredicate = PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(applTrendsTable, "APPLICATION_NAME"), new ValueExpression<String>("wealthApp_0"), BinaryOperator.EQ);
        Predicate serviceTypePredicate = PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(applTrendsTable, "SERVICE_TYPE"), new ValueExpression<String>("Service"), BinaryOperator.EQ);

        Predicate andAppSvcPredicate = PredicateFactory.createAndPredicate(applicationPredicate, serviceTypePredicate);

        Predicate compositePredicate = PredicateFactory.createAndPredicate(andAppSvcPredicate, andLevelEnvPredicate);

        Predicate finalCompositePredicate = PredicateFactory.createAndPredicate(compositePredicate, andDaysPredicate);

        Query<Predicate> queryModel = new QueryImpl<Predicate>();
        queryModel.setPredicate(finalCompositePredicate);

        return queryModel;
    }


    @Test
    public void evaluateAppsResponseTimeByHourQuery() {
        Query<Predicate> queryModel = buildAppsResponseTimeByHourQuery();
        executeAndAssertQuery(queryModel, 120, "AVGRESPONSETIME", "MINUTES", "ENVIRONMENT", "APPLICATION_NAME", "SERVICE_TYPE");
    }

     /*
     * query =  SELECT AVGRESPONSETIME, DAYS
     * 			FROM METRIC_APPLTRENDS
     * 			WHERE ENVIRONMENT = "environment_1" AND APPLICATION_NAME = "wealthApp_1" AND DIM_LEVEL_NAME = "minutes" AND HOURS >= last_hour AND HOURS <= this_hour
     * 			AND SERVICE_TYPE = "Service";
     */
    protected Query<Predicate> buildAppsResponseTimeByHourQuery() {
        String query = "SELECT AVGRESPONSETIME, DAYS FROM METRIC_APPLTRENDS WHERE ";

        long currHourDimValue =  TimeUtils.getThisHour(systemTime);
        System.out.println("this hour: "+ new Date(currHourDimValue));
        Predicate hourLEThisHour = PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(applTrendsTable, "HOURS"), new ValueExpression<Long>(currHourDimValue), BinaryOperator.LE);

        long prevHourDimValue = currHourDimValue - TimeUnit.MILLISECONDS.convert(60, TimeUnit.MINUTES);
        System.out.println("prev hour: "+ new Date(prevHourDimValue));
        Predicate hourGEPrevHour = PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(applTrendsTable, "HOURS"), new ValueExpression<Long>(prevHourDimValue), BinaryOperator.GE);

        Predicate andHoursPredicate = PredicateFactory.createAndPredicate(hourGEPrevHour, hourLEThisHour);

        Predicate dimNamePredicate = PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(applTrendsTable, "DIM_LEVEL_NAME"), new ValueExpression<String>("minutes"), BinaryOperator.EQ);
        Predicate environmentPredicate = PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(applTrendsTable, "ENVIRONMENT"), new ValueExpression<String>("environment_0"), BinaryOperator.EQ);

        Predicate andLevelEnvPredicate = PredicateFactory.createAndPredicate(dimNamePredicate, environmentPredicate);

        Predicate applicationPredicate = PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(applTrendsTable, "APPLICATION_NAME"), new ValueExpression<String>("wealthApp_0"), BinaryOperator.EQ);
        Predicate serviceTypePredicate = PredicateFactory.createBinaryPredicate(new SimpleQueryExpression(applTrendsTable, "SERVICE_TYPE"), new ValueExpression<String>("Service"), BinaryOperator.EQ);

        Predicate andAppSvcPredicate = PredicateFactory.createAndPredicate(applicationPredicate, serviceTypePredicate);

        Predicate compositePredicate = PredicateFactory.createAndPredicate(andAppSvcPredicate, andLevelEnvPredicate);

        Predicate finalCompositePredicate = PredicateFactory.createAndPredicate(compositePredicate, andHoursPredicate);

        Query<Predicate> queryModel = new QueryImpl<Predicate>();
        queryModel.setPredicate(finalCompositePredicate);

        return queryModel;
    }

    protected void printResults(QueryResultSet resultSet, String... attributes) {
        while (resultSet.hasNext()) {
        	MemoryTuple resultTuple = resultSet.next();
            StringBuilder resultStr = new StringBuilder();

            for (String attribute : attributes) {
                resultStr.append(attribute).append(" : ").append(resultTuple.getAttributeValue(attribute)).append(" , ");
            }
            System.out.println(resultStr.toString());
        }
    }

    protected void executeAndAssertQuery(Query<Predicate> queryModel, int expectedCount, String... attributes) {
        System.out.println("Query: "+ queryModel);
        QueryExecutor queryExecutor = new InMemoryQueryExecutor();

        long t1 = System.currentTimeMillis();

        QueryResultSet resultSet = queryExecutor.executeQuery(queryModel);

        long t2 = System.currentTimeMillis();

        System.out.println("Time for total " + (t2 - t1));

        System.out.println("--------Query Result: ---------------");

        printResults(resultSet, attributes);

        System.out.println("--------------------------");
        System.out.println("Total matching results: " + resultSet.totalCount());
        Assert.assertEquals(expectedCount, resultSet.totalCount());
    }
}
