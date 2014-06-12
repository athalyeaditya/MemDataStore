package org.mem.store.persistence;

import org.junit.Test;
import org.mem.store.query.model.OptimizerHint;
import org.mem.store.query.model.Predicate;
import org.mem.store.query.model.Query;

/**
 * Created by IntelliJ IDEA.
 * User: aathalye
 * Date: 21/12/13
 * Time: 3:23 PM
 *
 * Application trends with optimization enabled.
 */
public class OptimizedAppTrendsTableTest extends AppTrendsTableTest {

    @Override
    @Test
    public void evaluateAppsWeeklySummaryQuery() {
        Query<Predicate> queryModel = buildAppsWeeklySummaryQuery();
        queryModel.setOptimizerHint(OptimizerHint.COUNT);
        executeAndAssertQuery(queryModel, 5, "ENVIRONMENT", "APPLICATION_NAME", "SERVICE_TYPE", "HITCOUNT", "SUCCESSCOUNT", "FAULTCOUNT", "AVGRESPONSETIME", "WEEKS");
    }

    @Override
    @Test
    public void evaluateAppsResponseTimeByWeekQuery() {
        Query<Predicate> queryModel = buildAppsResponseTimeByWeekQuery();
        queryModel.setOptimizerHint(OptimizerHint.COUNT);
        executeAndAssertQuery(queryModel, 14, "AVGRESPONSETIME", "WEEKS", "ENVIRONMENT", "APPLICATION_NAME", "SERVICE_TYPE");
    }

    @Override
    @Test
    public void evaluateAppsResponseTimeByDayQuery() {
        Query<Predicate> queryModel = buildAppsResponseTimeByDayQuery();
        queryModel.setOptimizerHint(OptimizerHint.COUNT);
        executeAndAssertQuery(queryModel, 48, "AVGRESPONSETIME", "HOURS", "ENVIRONMENT", "APPLICATION_NAME", "SERVICE_TYPE");
    }

    @Override
    @Test
    public void evaluateAppsResponseTimeByHourQuery() {
        Query<Predicate> queryModel = buildAppsResponseTimeByHourQuery();
        queryModel.setOptimizerHint(OptimizerHint.COUNT);
        executeAndAssertQuery(queryModel, 120, "AVGRESPONSETIME", "MINUTES", "ENVIRONMENT", "APPLICATION_NAME", "SERVICE_TYPE");
    }
}
