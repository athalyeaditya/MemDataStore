package org.mem.store.persistence.model;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

public class TimeUtils {

    /*
     * get the week slice out of the timestamp:
     */
    public static long getThisWeek(long timestamp) {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(timestamp);

        int week_of_month = c.get(Calendar.WEEK_OF_MONTH);
        c.clear(Calendar.MILLISECOND);
        c.clear(Calendar.SECOND);
        c.clear(Calendar.MINUTE);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.DAY_OF_WEEK, 1);
        c.set(Calendar.WEEK_OF_MONTH, week_of_month);
        return c.getTimeInMillis();
    }

    /*
     * get the day slice out of the timestamp:
     */
    public static long getThisDay(long timestamp) {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(timestamp);

        int day = c.get(Calendar.DAY_OF_MONTH);
        c.set(Calendar.MILLISECOND, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.DAY_OF_MONTH, day);
        return c.getTimeInMillis();
    }

    /*
     * get the hour slice out of the timestamp:
     */
    public static long getThisHour(long timestamp) {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(timestamp);

        int hour = c.get(Calendar.HOUR_OF_DAY);
        c.set(Calendar.MILLISECOND, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.HOUR_OF_DAY, hour);
        return c.getTimeInMillis();
    }

    /*
     * get the minute slice out of the timestamp:
     */
    public static long getThisMinute(long timestamp) {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(timestamp);

        int minute = c.get(Calendar.MINUTE);
        c.set(Calendar.MILLISECOND, 0);
        c.set(Calendar.SECOND, 0);

        c.set(Calendar.MINUTE, minute);
        return c.getTimeInMillis();
    }


    public static List<TimeTreeNode> createTimeSliceTree(long timestamp, Map<TimeDimension, Integer> timeSliceCardinalityMap) {
        List<TimeTreeNode> weeksList = new ArrayList<TimeTreeNode>();
        // get the week slice out of the timestamp:
        long weekVal = getThisWeek(timestamp);

        for (int weekCnt = 0; weekCnt < timeSliceCardinalityMap.get(TimeDimension.WEEKS); weekCnt++) {
            long week = weekVal - (weekCnt * (1000 * 60 * 60 * 24 * 7));
            // create week node and add it to collection
            TimeTreeNode weekNode = new TimeTreeNode(TimeDimension.WEEKS, week);
            weeksList.add(weekNode);
            for (int daysInWeek = 0; daysInWeek < timeSliceCardinalityMap.get(TimeDimension.DAYS); daysInWeek++) {
                long day = week + (daysInWeek * (1000 * 60 * 60 * 24));
                // create day node and add it as a child of week node
                TimeTreeNode dayNode = new TimeTreeNode(TimeDimension.DAYS, day);
                weekNode.addChild(dayNode);
                for (int hrsInDay = 0; hrsInDay < timeSliceCardinalityMap.get(TimeDimension.HOURS); hrsInDay++) {
                    long hour = day + (hrsInDay * (1000 * 60 * 60));
                    // create hour node and add it as a child of day node
                    TimeTreeNode hourNode = new TimeTreeNode(TimeDimension.HOURS, hour);
                    dayNode.addChild(hourNode);
                    for (int minInHr = 0; minInHr < timeSliceCardinalityMap.get(TimeDimension.MINUTES); minInHr++) {
                        long minute = hour + (minInHr * (1000 * 60));
                        // create minute node and add it as a child of hour node
                        TimeTreeNode minuteNode = new TimeTreeNode(TimeDimension.MINUTES, minute);
                        hourNode.addChild(minuteNode);
                    }
                }
            }
        }

        return weeksList;
    }


}
