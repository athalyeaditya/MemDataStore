package org.mem.store.query.exec;

import org.junit.Assert;
import org.junit.Test;
import org.mem.store.persistence.model.MemoryKey;
import org.mem.store.persistence.model.MemoryTuple;
import org.mem.store.persistence.model.invm.impl.MemoryTupleImpl;
import org.mem.store.persistence.model.invm.impl.StringMemoryKey;
import org.mem.store.query.exec.util.StreamUtils;
import org.mem.store.query.model.impl.MutableResultStream;

import java.util.Collection;
import java.util.HashSet;

/**
 * Created by IntelliJ IDEA.
 * User: aathalye
 * Date: 13/12/13
 * Time: 11:55 AM
 * To change this template use File | Settings | File Templates.
 */
public class StreamUtilTest {

    private Collection<MemoryTuple> createTupleCollection(int start, int end) {
        Collection<MemoryTuple> tuplesCollection = new HashSet<MemoryTuple>();
        for (int i = start; i <= end; i++) {
            String key = "Key-" + i;
            MemoryKey memoryKey = new StringMemoryKey(key);
            MemoryTuple tuple1 = new MemoryTupleImpl(memoryKey);
            tuple1.setAttribute("environment", "env-" + i);
            tuple1.setAttribute("service", "service-" + i);
            tuple1.setAttribute("week", "week-" + i);
            tuplesCollection.add(tuple1);
        }
        return tuplesCollection;
    }

    @Test
    public void testSetUnion() {
        Collection<MemoryTuple> tupleCollection1 = createTupleCollection(0, 100000);
        Collection<MemoryTuple> tupleCollection2 = createTupleCollection(5000, 100000);
        MutableResultStream mutableResultStream1 = new MutableResultStream();
        mutableResultStream1.addMemoryTuples(tupleCollection1);
        MutableResultStream mutableResultStream2 = new MutableResultStream();
        mutableResultStream2.addMemoryTuples(tupleCollection2);
        MutableResultStream resultStream = StreamUtils.union(mutableResultStream1, mutableResultStream2);
        Assert.assertEquals(4, resultStream.getTuples().size());
    }
}
