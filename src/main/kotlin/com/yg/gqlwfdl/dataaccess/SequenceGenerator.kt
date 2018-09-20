package com.yg.gqlwfdl.dataaccess

import io.reactiverse.pgclient.PgClient
import org.jooq.DSLContext
import org.jooq.Sequence
import java.util.concurrent.CompletableFuture

interface SequenceGenerator<TId> {
    /**
     * Get next ID for the current table based on sequence parameter
     */
    fun getNextId(): CompletableFuture<TId>
}

class DBSequenceGenerator<TId : Number>(
        private val create: DSLContext,
        private val sequence: Sequence<TId>,
        private val pgClient: PgClient
) : SequenceGenerator<TId> {
    // This function only returns a single value, in practice it should take a size and return that many IDs
    // from a single query.
    override fun getNextId(): CompletableFuture<TId> {
        val sql = create
                .select(sequence.nextval())
                .sql

        val future = CompletableFuture<TId>()

        pgClient.query(sql) {
            future.complete(it.result().first().getValue(0) as TId)
        }

        return future
    }
}
