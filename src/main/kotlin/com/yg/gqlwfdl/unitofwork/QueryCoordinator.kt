package com.yg.gqlwfdl.unitofwork

import com.opidis.unitofwork.data.QueryCoordinator
import com.yg.gqlwfdl.dataaccess.PgClientExecutionInfo
import io.reactiverse.pgclient.PgPool
import org.springframework.stereotype.Component
import java.util.concurrent.CompletionStage
import java.util.logging.Level
import java.util.logging.Logger

@Component
class QueryCoordinator(private val pgPool: PgPool) : QueryCoordinator<QueryAction, PgClientExecutionInfo> {
    private val logger = Logger.getLogger(this.javaClass.canonicalName)
    override fun batchExecute(queries: List<QueryAction>, executionInfo: PgClientExecutionInfo?):
            CompletionStage<IntArray> {
        throw NotImplementedError()
    }

    override fun transaction(transactional: (PgClientExecutionInfo?) -> Unit) {
        pgPool.getConnection { connectionResult ->
            if (connectionResult.failed()) {
                logger?.log(Level.SEVERE, "Failed to get connection ${connectionResult.cause()}")
//                failureAction(asyncResultRowSet.cause())
                throw connectionResult.cause()
            }

            val connection = connectionResult.result()

            val transaction = connection.begin()

            val executionInfo = PgClientExecutionInfo(connection)



            transaction.commit()

        }
    }
}