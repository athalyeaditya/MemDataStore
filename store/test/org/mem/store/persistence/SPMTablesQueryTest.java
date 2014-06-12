package org.mem.store.persistence;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.mem.store.persistence.model.AppTrendsTableCreator;
import org.mem.store.persistence.model.TimeDimension;
import org.mem.store.persistence.model.TimeTreeNode;
import org.mem.store.persistence.model.TimeUtils;
import org.mem.store.persistence.service.invm.DataServiceFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class SPMTablesQueryTest {

    protected static String applTrendsTable;
    protected static final long systemTime = System.currentTimeMillis();
    protected static final String environmentBaseName = "environment";
    protected static final String applicationBaseName = "wealthApp";
    protected static final int envCount = 1;
    protected static final int appCount = 5;
    protected int serviceCount = 4;



    @BeforeClass
    public static void setUp() {
//        systemTime = System.currentTimeMillis();
        @SuppressWarnings("serial")
        Map<TimeDimension, Integer> timeSliceCardninalityMap = new HashMap<TimeDimension, Integer>() {{
            put(TimeDimension.WEEKS, 2);
            put(TimeDimension.DAYS, 7);
            put(TimeDimension.HOURS, 24);
            put(TimeDimension.MINUTES, 60);
        }};

        List<TimeTreeNode> timeTreeList = TimeUtils.createTimeSliceTree(systemTime, timeSliceCardninalityMap);
        for (TimeTreeNode weekNode : timeTreeList) {
            System.out.println("---week -------------");
            //weekNode.toString();
        }


        try {
            applTrendsTable = new AppTrendsTableCreator().createAndPopulateApplTrends(timeTreeList, environmentBaseName, applicationBaseName, envCount, appCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void cleanup() {
        DataServiceFactory.getInstance().getDataStoreService().clear(applTrendsTable);
        DataServiceFactory.getInstance().getMetaDataService().deleteTable(applTrendsTable);
    }
}
