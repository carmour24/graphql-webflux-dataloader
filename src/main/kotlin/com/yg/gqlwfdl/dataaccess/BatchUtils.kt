package com.yg.gqlwfdl.dataaccess

import io.reactiverse.pgclient.PgRowSet
import io.reactiverse.pgclient.Row
import io.vertx.core.AsyncResult
import org.jooq.Query
import org.jooq.conf.ParamType

/**
 * Iterate over all [PgRowSet] objects resulting from a batch operation.
 *
 * [AsyncResult] of PgRowSet doesn't provide an iterator over the rowset results. If we're batching inserts, there will
 * be a rowset per tuple of data passed to the batch operation.
 * The [PgRowSet] result does provide an iterator for each [Row] which can be used within the [action] to iterator
 * over the selected results.
 */
fun AsyncResult<PgRowSet>.forEachIndexed(action: (index: Int, rowSet: PgRowSet) -> Unit) {
    // We would expect the AsyncResult to have previously been interrogated for errors before enumerating the results
    // with this function so this is just here as a catch all in case it hasn't been for some reason.
    if (this.failed()) {
        println(this.cause())
        throw this.cause()
    }

    // Get the initial PgRowSet result.
    var result: PgRowSet? = this.result()
    var index = 0

    while (result != null) {
        // Invoke the action with the index and current set of result rows produced by the corresponding item for
        // the batch operation.
        action(index++, result)

        // Look for subsequent row set from the next item in the batch operation.
        result = result.next()
    }
}

fun AsyncResult<PgRowSet>.flattenRows(): List<Row> {
    // We would expect the AsyncResult to have previously been interrogated for errors before enumerating the results
    // with this function so this is just here as a catch all in case it hasn't been for some reason.
    if (this.failed()) {
        println(this.cause())
        throw this.cause()
    }

    // Get the initial PgRowSet result.
    var result: PgRowSet? = this.result()
    val rows = emptyList<Row>().toMutableList()


    while (result != null) {
        // Invoke the action with the index and current set of result rows produced by the corresponding item for
        // the batch operation.
        rows.addAll(rows.size, result.toList())

        // Look for subsequent row set from the next item in the batch operation.
        result = result.next()
    }

    return rows
}
