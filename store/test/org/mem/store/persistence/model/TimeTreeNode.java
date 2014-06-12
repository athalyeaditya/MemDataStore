package org.mem.store.persistence.model;

import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

public class TimeTreeNode implements Iterable<TimeTreeNode> {

    TimeDimension level;
    long timeSliceValue;
    private Set<TimeTreeNode> children;


    public TimeTreeNode(TimeDimension level, long timeValue) {
        super();
        this.level = level;
        this.timeSliceValue = timeValue;
        children = new LinkedHashSet<TimeTreeNode>();
    }

    public boolean addChild(TimeTreeNode n) {
        return children.add(n);
    }

    public boolean removeChild(TimeTreeNode n) {
        return children.remove(n);
    }

    @Override
    public Iterator<TimeTreeNode> iterator() {
        return children.iterator();
    }

    @Override
    public boolean equals(Object node) {
        if (node == null) {
            return false;
        }

        if (!(node instanceof TimeTreeNode)) {
            return false;
        }
        TimeTreeNode tNode = (TimeTreeNode) node;
        if (!(this.level.equals(tNode.level))) {
            return false;
        }
        if (!(this.timeSliceValue == tNode.timeSliceValue)) {
            return false;
        }
        return true;
    }


    @Override
    public int hashCode() {
        return super.hashCode() + level.hashCode() + (int) timeSliceValue;
    }

    public TimeDimension getLevel() {
        return level;
    }

    public void setLevel(TimeDimension level) {
        this.level = level;
    }

    public long getTimeSliceValue() {
        return timeSliceValue;
    }

    public void setTimeSliceValue(long timeSliceValue) {
        this.timeSliceValue = timeSliceValue;
    }

    @Override
    public String toString() {
        switch (level) {
            case WEEKS:
                System.out.println("Week: " + new Date(timeSliceValue));
                break;
            case DAYS:
                System.out.println("\tDay: " + new Date(timeSliceValue));
                break;
            case HOURS:
                System.out.println("\t\tHour: " + new Date(timeSliceValue));
                break;
            case MINUTES:
                System.out.println("\t\t\tMinute: " + new Date(timeSliceValue));
                break;
        }
        for (TimeTreeNode child : children) {
            child.toString();
        }
        return "";
    }

}
