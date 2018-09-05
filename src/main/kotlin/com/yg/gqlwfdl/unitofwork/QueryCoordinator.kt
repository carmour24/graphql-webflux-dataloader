package com.yg.gqlwfdl.unitofwork

import com.opidis.unitofwork.data.QueryCoordinator

class QueryCoordinator : QueryCoordinator<QueryAction> {
    override fun batchExecute(queries: List<QueryAction>): IntArray {
        throw NotImplementedError()
    }

    override fun transaction(transactional: QueryAction) {
        throw NotImplementedError()
    }

}