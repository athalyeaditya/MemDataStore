package org.mem.store.query.model;

import org.mem.store.persistence.model.MemoryTuple;

import java.util.Collection;

/**
 * Created by IntelliJ IDEA.
 * User: aathalye
 * Date: 27/11/13
 * Time: 11:42 AM
 *
 */
public interface ResultStream {

    /**
     *
     *
     */
    public <T extends MemoryTuple> void addMemoryTuples(T... tuples);

    /**
     *
     *
     */
    public <T extends MemoryTuple> void addMemoryTuples(Collection<T> tuples);

    /**
     *
     *
     */
    public <T extends MemoryTuple> Collection<T> getTuples();
}
