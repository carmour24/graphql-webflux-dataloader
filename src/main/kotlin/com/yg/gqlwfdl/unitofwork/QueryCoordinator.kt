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

typealias TransactionCompletion = () -> CompletableFuture<Void>

@Component
class QueryCoordinator(private val pgPool: PgPool) : QueryCoordinator<QueryAction, PgClientExecutionInfo> {
    private val logger by lazy { getLogger() }
    var connection: PgConnection? = null
    var transaction: PgTransaction? = null

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

    override fun transaction(transactional: (PgClientExecutionInfo?) -> Unit): TransactionCompletion {
        pgPool.getConnection { connectionResult ->
            if (connectionResult.failed()) {
                logger?.log(Level.SEVERE, "Failed to get connection ${connectionResult.cause()}")
                throw connectionResult.cause()
            }

            val localConnection = connectionResult.result()
            connection = localConnection

            // If a transaction does not already exist then start a new one
            transaction = localConnection.begin()

            val executionInfo = PgClientExecutionInfo(localConnection)

            transactional(executionInfo)
        }

        logger?.log(Level.FINE, "Returning done")
        return {
            logger?.log(Level.FINE, "Calling commitTransactionAndReturnConnectionToPool")

            commitTransactionAndReturnConnectionToPool(transaction!!, connection!!)
        }
    }

    fun commitTransactionAndReturnConnectionToPool(transaction: PgTransaction, connection: PgConnection): CompletableFuture<Void> {
        val future = CompletableFuture<Void>()

        logger?.log(Level.FINE, "Transaction committing")

        transaction.commit {
            if (it.failed()) {
                logger?.log(Level.SEVERE, "Commit failed ${it.cause()}")
            }
            logger?.log(Level.FINE, "Transaction committed")

            connection.exceptionHandler {
                logger?.log(Level.FINE, "Exception $it")
            }

            logger?.log(Level.FINER, "Connection closing")
            connection.close()
            logger?.log(Level.FINER, "Connection closed")

            future.complete(null)
        }

        return future
    }
}