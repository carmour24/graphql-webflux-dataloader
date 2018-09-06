package com.yg.gqlwfdl.unitofwork

import com.opidis.unitofwork.data.QueryCoordinator
import com.yg.gqlwfdl.dataaccess.PgClientExecutionInfo
import java.util.concurrent.CompletionStage

class QueryCoordinator : QueryCoordinator<QueryAction, PgClientExecutionInfo> {
    override fun batchExecute(queries: List<QueryAction>, executionInfo: PgClientExecutionInfo?):
            CompletionStage<IntArray> {
        throw NotImplementedError()
    }

    override fun transaction(transactional: () -> Unit){

    }
}