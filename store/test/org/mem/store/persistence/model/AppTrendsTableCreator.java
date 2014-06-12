package org.mem.store.persistence.model;

import org.mem.store.persistence.descriptor.ColumnDescriptor;
import org.mem.store.persistence.descriptor.impl.DescriptorFactory;
import org.mem.store.persistence.model.invm.impl.MemoryTupleImpl;
import org.mem.store.persistence.model.invm.impl.StringMemoryKey;
import org.mem.store.persistence.model.listeners.MemoryTableChangeObserver;
import org.mem.store.persistence.model.listeners.MemoryTableListener;
import org.mem.store.persistence.service.MetadataService;
import org.mem.store.persistence.service.invm.DataServiceFactory;
import org.mem.store.query.model.DataType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class AppTrendsTableCreator {

    public String createAndPopulateApplTrends(List<TimeTreeNode> timeTreeList, String environmentBaseName,
                                              String applicationBaseName, int envCount, int appCount) throws Exception {


        String[] serviceTypes = {"Service", "Reference"};

        String tableName = "METRIC_APPLTRENDS";

        String[] applTrendsCols = {"DIM_LEVEL", "DIM_LEVEL_NAME", "ENVIRONMENT", "APPLICATION_NAME", "SERVICE_TYPE",
                "WEEKS", "DAYS", "HOURS", "MINUTES", "HITCOUNT", "SUCCESSCOUNT", "FAULTCOUNT",
                "AVGRESPONSETIME", "AVGRESPONSETIME_COUNT", "AVGRESPONSETIME_SUM",
                "TP5RESPONSETIME", "TP5RESPONSETIME_COUNT", "TP5RESPONSETIME_SUM", "TP5RESPONSETIME_SUMOFSQUARE",
                "TP95RESPONSETIME", "TP95RESPONSETIME_COUNT", "TP95RESPONSETIME_SUM", "TP95RESPONSETIME_SUMOFSQUARE",
                "CREATED_TIME", "UPDATED_TIME"
        };
        List<ColumnDescriptor> applTrendsColumns = new ArrayList<ColumnDescriptor>() {{
            add(DescriptorFactory.createColumnDescriptor("DIM_LEVEL_NAME", DataType.STRING, true));
            add(DescriptorFactory.createColumnDescriptor("ENVIRONMENT", DataType.STRING, true));
            add(DescriptorFactory.createColumnDescriptor("APPLICATION_NAME", DataType.STRING, true));
            add(DescriptorFactory.createColumnDescriptor("SERVICE_TYPE", DataType.STRING, true));
            add(DescriptorFactory.createColumnDescriptor("WEEKS", DataType.LONG, true));
            add(DescriptorFactory.createColumnDescriptor("DAYS", DataType.LONG, true));
            add(DescriptorFactory.createColumnDescriptor("HOURS", DataType.LONG, true));
            add(DescriptorFactory.createColumnDescriptor("MINUTES", DataType.LONG, true));
        }};

        MetadataService metadataService = DataServiceFactory.getInstance().getMetaDataService();
        metadataService.createTable(DescriptorFactory.createMemoryTableDescriptor(tableName));
        
        // Register table change listener
        MemoryTableListener appTrendsTableListener = new MemoryTableChangeObserver();
        metadataService.registerTableListener(tableName, appTrendsTableListener);
        
        for (ColumnDescriptor columnDescriptor : applTrendsColumns) {
            metadataService.createIndex(tableName, columnDescriptor);
        }
        
        for (int envNum = 0; envNum < envCount; envNum++) {
            for (int appNum = 0; appNum < appCount; appNum++) {
                for (String serviceType : serviceTypes) {
                    String envName = environmentBaseName + "_" + envNum;
                    String appName = applicationBaseName + "_" + appNum;

                    for (TimeTreeNode timeTree : timeTreeList) {
                        TimeTreeNode weekNode = timeTree;
                        long weekValue = weekNode.timeSliceValue;
                        // create week tuple
                        createTuple(tableName, envName, appName, serviceType, TimeDimension.WEEKS, weekValue, -1L, -1L, -1L, applTrendsCols);
                        Iterator<TimeTreeNode> dayNodeIter = weekNode.iterator();
                        while (dayNodeIter != null && dayNodeIter.hasNext()) {
                            TimeTreeNode dayNode = dayNodeIter.next();
                            long dayValue = dayNode.timeSliceValue;
                            // create day tuple
                            createTuple(tableName, envName, appName, serviceType, TimeDimension.DAYS, weekValue, dayValue, -1L, -1L, applTrendsCols);
                            Iterator<TimeTreeNode> hrNodeIter = dayNode.iterator();
                            while (hrNodeIter != null && hrNodeIter.hasNext()) {
                                TimeTreeNode hrNode = hrNodeIter.next();
                                long hrValue = hrNode.timeSliceValue;
                                // create hour tuple
                                createTuple(tableName, envName, appName, serviceType, TimeDimension.HOURS, weekValue, dayValue, hrValue, -1L, applTrendsCols);
                                Iterator<TimeTreeNode> minNodeIter = hrNode.iterator();
                                while (minNodeIter != null && minNodeIter.hasNext()) {
                                    TimeTreeNode minNode = minNodeIter.next();
                                    long minValue = minNode.timeSliceValue;
                                    // create minute tuple
                                    createTuple(tableName, envName, appName, serviceType, TimeDimension.MINUTES, weekValue, dayValue, hrValue, minValue, applTrendsCols);
                                }
                            }
                        }
                    }
                }
            }
        }
        
        System.out.println("Table: "+ tableName+ " created..");
        System.out.println(tableName+ " statistics: ");
        System.out.println("Total number of tuples in table: "+DataServiceFactory.getInstance().getDataStoreService().getSizeOfTable(tableName));
        System.out.println("Table Indexes: ");
        Iterator<String> indexNamesIterator = metadataService.getIndexNames(tableName);
        while(indexNamesIterator != null && indexNamesIterator.hasNext()) {
        	String indexName = indexNamesIterator.next();
        	int indexCardinality = metadataService.getCardinality(tableName, indexName);
        	System.out.println("Index: "+ indexName+ ", cardinality: "+ indexCardinality);        	
        }
        
        long tableSizeInBytes = appTrendsTableListener.getTableSizeInBytes();
        System.out.println("Table size : "+ tableSizeInBytes+ " bytes = "+tableSizeInBytes/1024+ " KB = "+  tableSizeInBytes/(1024*1024) + " MB");
        return tableName;
    }


    private void createTuple(String tableName, String envName, String appName,
                             String serviceType, TimeDimension timeDim, long weekValue, long dayValue,
                             long hourValue, long minuteValue, String[] applTrendsCols) {
        switch (timeDim) {
            case WEEKS:
                StringBuilder metricKeySB = new StringBuilder();
                metricKeySB.append("weeks/").append(envName).append("/").append(appName).append("/").append(serviceType).append("/").append(weekValue);
                MemoryKey metricKey = new StringMemoryKey(metricKeySB.toString());
                MemoryTuple tuple = new MemoryTupleImpl(metricKey);
                tuple.setAttribute(applTrendsCols[0], 3);
                tuple.setAttribute(applTrendsCols[1], "weeks");
                tuple.setAttribute(applTrendsCols[2], envName);
                tuple.setAttribute(applTrendsCols[3], appName);
                tuple.setAttribute(applTrendsCols[4], serviceType);
                tuple.setAttribute(applTrendsCols[5], weekValue);
                tuple.setAttribute(applTrendsCols[6], null);
                tuple.setAttribute(applTrendsCols[7], null);
                tuple.setAttribute(applTrendsCols[8], null);
                tuple.setAttribute(applTrendsCols[9], 3147.0);
                tuple.setAttribute(applTrendsCols[10], 2787.0);
                tuple.setAttribute(applTrendsCols[11], 360.0);
                tuple.setAttribute(applTrendsCols[12], 410.4);
                tuple.setAttribute(applTrendsCols[13], 120);
                tuple.setAttribute(applTrendsCols[14], 49248.0);
                tuple.setAttribute(applTrendsCols[15], 192.52725940945024);
                tuple.setAttribute(applTrendsCols[16], 120);
                tuple.setAttribute(applTrendsCols[17], 49248.0);
                tuple.setAttribute(applTrendsCols[18], 26175024.99890);
                tuple.setAttribute(applTrendsCols[19], 628.2727405905498);
                tuple.setAttribute(applTrendsCols[20], 120);
                tuple.setAttribute(applTrendsCols[21], 49248.0);
                tuple.setAttribute(applTrendsCols[22], 26175024.99890);
                tuple.setAttribute(applTrendsCols[23], "2013-12-11 13:57:40.01");
                tuple.setAttribute(applTrendsCols[24], "2013-12-11 13:57:40.01");

                DataServiceFactory.getInstance().getDataStoreService().put(tableName, tuple);
                //System.out.println("created tuple: "+ tuple.toString());
                break;
            case DAYS:
                metricKeySB = new StringBuilder();
                metricKeySB.append("days/").append(envName).append("/").append(appName).append("/").
                        append(serviceType).append("/").append(weekValue).append("/").append(dayValue);
                metricKey = new StringMemoryKey(metricKeySB.toString());
                tuple = new MemoryTupleImpl(metricKey);
                tuple.setAttribute(applTrendsCols[0], 4);
                tuple.setAttribute(applTrendsCols[1], "days");
                tuple.setAttribute(applTrendsCols[2], envName);
                tuple.setAttribute(applTrendsCols[3], appName);
                tuple.setAttribute(applTrendsCols[4], serviceType);
                tuple.setAttribute(applTrendsCols[5], weekValue);
                tuple.setAttribute(applTrendsCols[6], dayValue);
                tuple.setAttribute(applTrendsCols[7], null);
                tuple.setAttribute(applTrendsCols[8], null);
                tuple.setAttribute(applTrendsCols[9], 3147.0);
                tuple.setAttribute(applTrendsCols[10], 2787.0);
                tuple.setAttribute(applTrendsCols[11], 360.0);
                tuple.setAttribute(applTrendsCols[12], 410.4);
                tuple.setAttribute(applTrendsCols[13], 120);
                tuple.setAttribute(applTrendsCols[14], 49248.0);
                tuple.setAttribute(applTrendsCols[15], 192.52725940945024);
                tuple.setAttribute(applTrendsCols[16], 120);
                tuple.setAttribute(applTrendsCols[17], 49248.0);
                tuple.setAttribute(applTrendsCols[18], 26175024.99890);
                tuple.setAttribute(applTrendsCols[19], 628.2727405905498);
                tuple.setAttribute(applTrendsCols[20], 120);
                tuple.setAttribute(applTrendsCols[21], 49248.0);
                tuple.setAttribute(applTrendsCols[22], 26175024.99890);
                tuple.setAttribute(applTrendsCols[23], "2013-12-11 13:57:40.01");
                tuple.setAttribute(applTrendsCols[24], "2013-12-11 13:57:40.01");

                DataServiceFactory.getInstance().getDataStoreService().put(tableName, tuple);                
                //System.out.println("created tuple: "+ tuple.toString());
                break;

            case HOURS:
                metricKeySB = new StringBuilder();
                metricKeySB.append("hours/").append(envName).append("/").append(appName).append("/").
                        append(serviceType).append("/").append(weekValue).append("/").append(dayValue).append("/").append(hourValue);
                metricKey = new StringMemoryKey(metricKeySB.toString());
                tuple = new MemoryTupleImpl(metricKey);
                tuple.setAttribute(applTrendsCols[0], 5);
                tuple.setAttribute(applTrendsCols[1], "hours");
                tuple.setAttribute(applTrendsCols[2], envName);
                tuple.setAttribute(applTrendsCols[3], appName);
                tuple.setAttribute(applTrendsCols[4], serviceType);
                tuple.setAttribute(applTrendsCols[5], weekValue);
                tuple.setAttribute(applTrendsCols[6], dayValue);
                tuple.setAttribute(applTrendsCols[7], hourValue);
                tuple.setAttribute(applTrendsCols[8], null);
                tuple.setAttribute(applTrendsCols[9], 3147.0);
                tuple.setAttribute(applTrendsCols[10], 2787.0);
                tuple.setAttribute(applTrendsCols[11], 360.0);
                tuple.setAttribute(applTrendsCols[12], 410.4);
                tuple.setAttribute(applTrendsCols[13], 120);
                tuple.setAttribute(applTrendsCols[14], 49248.0);
                tuple.setAttribute(applTrendsCols[15], 192.52725940945024);
                tuple.setAttribute(applTrendsCols[16], 120);
                tuple.setAttribute(applTrendsCols[17], 49248.0);
                tuple.setAttribute(applTrendsCols[18], 26175024.99890);
                tuple.setAttribute(applTrendsCols[19], 628.2727405905498);
                tuple.setAttribute(applTrendsCols[20], 120);
                tuple.setAttribute(applTrendsCols[21], 49248.0);
                tuple.setAttribute(applTrendsCols[22], 26175024.99890);
                tuple.setAttribute(applTrendsCols[23], "2013-12-11 13:57:40.01");
                tuple.setAttribute(applTrendsCols[24], "2013-12-11 13:57:40.01");

                DataServiceFactory.getInstance().getDataStoreService().put(tableName, tuple);
                //System.out.println("created tuple: "+ tuple.toString());
                break;

            case MINUTES:
                metricKeySB = new StringBuilder();
                metricKeySB.append("minutes/").append(envName).append("/").append(appName).append("/").
                        append(serviceType).append("/").append(weekValue).append("/").append(dayValue).append("/").append(hourValue).append("/").append(minuteValue);
                metricKey = new StringMemoryKey(metricKeySB.toString());
                tuple = new MemoryTupleImpl(metricKey);
                tuple.setAttribute(applTrendsCols[0], 6);
                tuple.setAttribute(applTrendsCols[1], "minutes");
                tuple.setAttribute(applTrendsCols[2], envName);
                tuple.setAttribute(applTrendsCols[3], appName);
                tuple.setAttribute(applTrendsCols[4], serviceType);
                tuple.setAttribute(applTrendsCols[5], weekValue);
                tuple.setAttribute(applTrendsCols[6], dayValue);
                tuple.setAttribute(applTrendsCols[7], hourValue);
                tuple.setAttribute(applTrendsCols[8], minuteValue);
                tuple.setAttribute(applTrendsCols[9], 3147.0);
                tuple.setAttribute(applTrendsCols[10], 2787.0);
                tuple.setAttribute(applTrendsCols[11], 360.0);
                tuple.setAttribute(applTrendsCols[12], 410.4);
                tuple.setAttribute(applTrendsCols[13], 120);
                tuple.setAttribute(applTrendsCols[14], 49248.0);
                tuple.setAttribute(applTrendsCols[15], 192.52725940945024);
                tuple.setAttribute(applTrendsCols[16], 120);
                tuple.setAttribute(applTrendsCols[17], 49248.0);
                tuple.setAttribute(applTrendsCols[18], 26175024.99890);
                tuple.setAttribute(applTrendsCols[19], 628.2727405905498);
                tuple.setAttribute(applTrendsCols[20], 120);
                tuple.setAttribute(applTrendsCols[21], 49248.0);
                tuple.setAttribute(applTrendsCols[22], 26175024.99890);
                tuple.setAttribute(applTrendsCols[23], "2013-12-11 13:57:40.01");
                tuple.setAttribute(applTrendsCols[24], "2013-12-11 13:57:40.01");

                DataServiceFactory.getInstance().getDataStoreService().put(tableName, tuple);
                //System.out.println("created tuple: "+ tuple.toString());
                break;
        }
    }

}
