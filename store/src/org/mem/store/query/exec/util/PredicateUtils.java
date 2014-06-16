package org.mem.store.query.exec.util;

import com.google.common.base.Predicate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * Created by IntelliJ IDEA.
 * User: aathalye
 * Date: 11/12/13
 * Time: 3:43 PM
 *
 */
public class PredicateUtils {

    public static <T> Collection<T> createCollectionFromIterator(Iterator<T> iterator) {
        return createCollectionFromIterator(iterator, new Predicate<T>() {
            @Override
            public boolean apply(T input) {
                //No op
                return true;
            }
        });
    }

    /**
     * Filter a collection from iterator based on a predicate.
     */
    public static <T, P extends Predicate<T>> Collection<T> createCollectionFromIterator(Iterator<T> iterator, P predicate) {
        Collection<T> tuplesCollection = new ArrayList<T>();

        while (iterator.hasNext()) {
            T next = iterator.next();
            if (predicate.apply(next)) {
                tuplesCollection.add(next);
            }
        }
        return tuplesCollection;
    }

    /**
     * The apache commons works only on modifiable collections.
     */
    public static <T, P extends Predicate<T>> Collection<T> filter(Collection<T> collection, P predicate) {
        if (collection == null || predicate == null) {
            throw new IllegalArgumentException("Collection to be filtered and predicate cannot be null");
        }
        Iterator<T> iterator = collection.iterator();
        return createCollectionFromIterator(iterator, predicate);
    }
}
