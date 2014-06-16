package org.mem.store.query.exec;

import org.mem.store.query.model.Predicate;
import org.mem.store.query.model.Query;
import org.mem.store.query.model.QueryResultSet;

/**
 * Created by IntelliJ IDEA.
 * User: aathalye
 * Date: 28/11/13
 * Time: 12:03 PM
 *
 * Generic query executor. Each Persistence layer will implement this.
 *
 */
public interface QueryExecutor {

    /**
     * Executes a query definition and returns a result set
     * Clients can query the result set using hasNext and next APIs.
     */
    public QueryResultSet executeQuery(Query<Predicate> query);
}
