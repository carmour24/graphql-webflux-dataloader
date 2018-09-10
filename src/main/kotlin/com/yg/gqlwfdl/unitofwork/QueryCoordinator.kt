package com.yg.gqlwfdl.unitofwork

import com.opidis.unitofwork.data.QueryCoordinator
import com.yg.gqlwfdl.dataaccess.PgClientExecutionInfo
import com.yg.gqlwfdl.getLogger
import io.reactiverse.pgclient.PgConnection
import io.reactiverse.pgclient.PgPool
import io.reactiverse.pgclient.PgTransaction
import org.springframework.stereotype.Component
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletableFuture.allOf
import java.util.concurrent.CompletionStage
import java.util.logging.Level
import java.util.logging.Logger


@Component
class QueryCoordinator(private val pgPool: PgPool) : QueryCoordinator<QueryAction, PgClientExecutionInfo> {
    private val logger by lazy { getLogger() }

    override fun batchExecute(queries: List<QueryAction>, executionInfo: PgClientExecutionInfo?):
            CompletionStage<IntArray> {
        val queryFutures = queries.map {
            it.invoke(executionInfo).toCompletableFuture()
        }

        return allOf(*queryFutures.toTypedArray()).thenCompose {
            queryFutures.reduceRight { completableFuture, acc ->
                val combinedArrays = completableFuture.get() + acc.get()
                val future = CompletableFuture<IntArray>()
                future.complete(combinedArrays)
                future
            }
        }
    }

    override fun transaction(transactional: (PgClientExecutionInfo?) -> Unit): () -> Unit {
        var connection: PgConnection? = null
        var transaction: PgTransaction? = null

        pgPool.getConnection { connectionResult ->
            if (connectionResult.failed()) {
                logger?.log(Level.SEVERE, "Failed to get connection ${connectionResult.cause()}")
                throw connectionResult.cause()
            }

            connection = connectionResult.result()

            connection!!.begin()

            val executionInfo = PgClientExecutionInfo(connection!!)

            transactional(executionInfo)
        }
        return {
            transaction!!.commit()
            connection!!.close()
        }
    }
}