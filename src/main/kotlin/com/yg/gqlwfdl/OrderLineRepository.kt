package com.yg.gqlwfdl.dataaccess

import com.yg.gqlwfdl.dataaccess.db.Sequences
import com.yg.gqlwfdl.dataaccess.db.Tables.ORDER_LINE
import com.yg.gqlwfdl.dataaccess.db.Tables.PRODUCT
import com.yg.gqlwfdl.dataaccess.db.tables.records.OrderLineRecord
import com.yg.gqlwfdl.dataaccess.db.tables.records.ProductRecord
import com.yg.gqlwfdl.dataaccess.joins.ClientFieldToJoinMapper
import com.yg.gqlwfdl.dataaccess.joins.JoinedRecordToEntityConverterProvider
import com.yg.gqlwfdl.dataaccess.joins.RecordProvider
import com.yg.gqlwfdl.getLogger
import com.yg.gqlwfdl.services.LineID
import com.yg.gqlwfdl.services.Order
import io.reactiverse.pgclient.PgPool
import org.jooq.Condition
import org.jooq.DSLContext
import org.jooq.Table
import org.springframework.stereotype.Repository
import java.util.concurrent.CompletableFuture
import java.util.logging.Level

interface OrderLineRepository : EntityRepository<Order.Line, LineID>, MutatingRepository<Order.Line, LineID,
        PgClientExecutionInfo>

@Repository
class DBOrderLineRepository(create: DSLContext,
                            connectionPool: PgPool,
                            recordToEntityConverterProvider: JoinedRecordToEntityConverterProvider,
                            clientFieldToJoinMapper: ClientFieldToJoinMapper,
                            recordProvider: RecordProvider)
    : DBEntityRepository<Order.Line, LineID, OrderLineRecord, OrderLineQueryInfo>(
        create,
        connectionPool,
        recordToEntityConverterProvider,
        clientFieldToJoinMapper,
        recordProvider,
        ORDER_LINE,
        ORDER_LINE.ID),
        MutatingRepository<Order.Line, LineID, PgClientExecutionInfo> by DBMutatingEntityRepository(
                create = create,
                pgClient = connectionPool,
                table = ORDER_LINE,
                sequence = Sequences.ORDER_LINE_ID_SEQ),
        OrderLineRepository {
    private val logger = getLogger()

    override fun find(requestInfo: EntityRequestInfo?, conditionProvider: ((OrderLineQueryInfo) -> List<Condition>)?): CompletableFuture<List<Order.Line>> {
        val queryInfo = OrderLineQueryInfo(ORDER_LINE)

        val joinInstanceSets = requestInfo?.let {
            listOf(it.getJoinInstanceSet(JoinRequestSource(queryInfo.primaryTable), queryInfo),
                    it.getJoinInstanceSet(JoinRequestSource(queryInfo.productTable, "lines/product"), queryInfo))
        } ?: listOf()

        val select = create
                .select(queryInfo.getAllFields())
                .from(queryInfo.primaryTable)
                .innerJoin(queryInfo.productTable)
                .on(queryInfo.primaryTable.field(ORDER_LINE.PRODUCT).eq(queryInfo.productTable.field(PRODUCT.ID)))

        // Add the joins to the query.
        joinInstanceSets.forEach { set -> set.joinInstances.forEach { it.join(select) } }

        // Add sorting to ensure that all items with same order ID are next to each other.
        // TODO: allow for custom sorting first, limits, etc., for paging.
        val finalQuery = select
                .withConditions(conditionProvider?.invoke(queryInfo))
                .orderBy(queryInfo.primaryTable.field(ORDER_LINE.ID))

        return finalQuery.fetchRowsAsync().thenApply { rows ->
            //            getEntities(queryInfo, it, requestInfo?.entityCreationListener, joinInstanceSets)
            for (row in rows) {
                logger?.log(Level.FINER, "$row")
            }
            emptyList<Order.Line>()
       }
    }
}

class OrderLineQueryInfo(table: Table<OrderLineRecord>) : QueryInfo<OrderLineRecord>(table) {
    val productTable: Table<ProductRecord> = addJoinedTable(PRODUCT, primaryTable, "prod", false)
}
