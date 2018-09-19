package com.yg.gqlwfdl.dataaccess

import com.yg.gqlwfdl.dataaccess.db.Sequences
import com.yg.gqlwfdl.dataaccess.db.Tables.*
import com.yg.gqlwfdl.dataaccess.db.tables.records.OrderLineRecord
import com.yg.gqlwfdl.dataaccess.db.tables.records.OrderRecord
import com.yg.gqlwfdl.dataaccess.db.tables.records.ProductRecord
import com.yg.gqlwfdl.dataaccess.joins.ClientFieldToJoinMapper
import com.yg.gqlwfdl.dataaccess.joins.JoinedRecordToEntityConverterProvider
import com.yg.gqlwfdl.dataaccess.joins.RecordProvider
import com.yg.gqlwfdl.dataloaders.customerOrderDataLoader
import com.yg.gqlwfdl.services.Customer
import com.yg.gqlwfdl.services.Order
import com.yg.gqlwfdl.services.OrderID
import io.reactiverse.pgclient.PgPool
import io.reactiverse.pgclient.Row
import org.jooq.Condition
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.Table
import org.springframework.stereotype.Repository
import java.util.concurrent.CompletableFuture

/**
 * Repository providing access to order information.
 */
interface OrderRepository : EntityRepository<Order, Long>, MutatingRepository<Order, OrderID, PgClientExecutionInfo> {
    /**
     * Returns a [CompletableFuture] which, when completed, will provide a [List] of all [Order] objects belonging
     * to [Customer]s with the passed in [customerIds].
     *
     * @param requestInfo Information about the request, such as the fields of the entity which were requested by the
     * client, if the call was made from the context of a client request.
     */
    fun findByCustomerIds(customerIds: List<Long>, requestInfo: EntityRequestInfo? = null):
            CompletableFuture<List<Order>>
}

/**
 * Concrete implementation of [OrderRepository], which uses a database for its data.
 */
@Repository
class DBOrderRepository(create: DSLContext,
                        connectionPool: PgPool,
                        recordToEntityConverterProvider: JoinedRecordToEntityConverterProvider,
                        clientFieldToJoinMapper: ClientFieldToJoinMapper,
                        recordProvider: RecordProvider)
    : DBEntityRepository<Order, Long, OrderRecord, OrderQueryInfo>(
                create,
                connectionPool,
                recordToEntityConverterProvider,
                clientFieldToJoinMapper,
                recordProvider,
                ORDER,
                ORDER.ID ),
        MutatingRepository<Order, OrderID, PgClientExecutionInfo> by DBMutatingEntityRepository(
                create,
                connectionPool,
                ORDER ),
        OrderRepository {

    override fun getNextId(): Long {
        //TODO: Finish!
        val create.select(Sequences.ORDER_ID_SEQ.nextval()).from().sql
    }

    override fun findByCustomerIds(customerIds: List<Long>, requestInfo: EntityRequestInfo?): CompletableFuture<List<Order>> {
        // Find all orders for the supplied customer IDs. The "find" method will cache the individual orders ...
        return find(requestInfo) { queryInfo -> listOf(queryInfo.primaryTable.field(ORDER.CUSTOMER).`in`(customerIds)) }
                // ... but when we have the results, before returning them first cache the orders which belong to each
                // customer. i.e. for each customer add a cache entry whose ID is the customer ID, and whose value is a
                // list of the orders belonging to that customer.
                .whenComplete { orders, _ ->
                    if (orders != null && requestInfo?.context != null) {
                        // Group the orders by the customer ID then for each entry (i.e. each customer) cache its
                        // orders.
                        orders.groupBy { it.customerId }.forEach {
                            requestInfo.context.customerOrderDataLoader.prime(it.key, it.value)
                        }
                    }
                }
    }

    override fun find(requestInfo: EntityRequestInfo?, conditionProvider: ((OrderQueryInfo) -> List<Condition>)?)
            : CompletableFuture<List<Order>> {

        val queryInfo = OrderQueryInfo(ORDER)

        // Get all the join joinInstances required based on the requestInfo. Allow joins to initiate from the primary
        // table (ORDER), the ORDER_LINE table (for children of the "lines" client field), and the PRODUCT table (for
        // children of the "lines/product" client field.
        val joinInstanceSets = requestInfo?.let {
            listOf(it.getJoinInstanceSet(JoinRequestSource(queryInfo.primaryTable), queryInfo),
                    it.getJoinInstanceSet(JoinRequestSource(queryInfo.orderLineTable, "lines"), queryInfo),
                    it.getJoinInstanceSet(JoinRequestSource(queryInfo.productTable, "lines/product"), queryInfo))
        } ?: listOf()

        // Build up the "SELECT... FROM..." part of the query. This query joins ORDER to ORDER_LINE to PRODUCT, so will
        // return one row per order line: we loop through these and group them by the order ID to build the individual
        // orders.
        val select = create
                .select(queryInfo.getAllFields())
                .from(queryInfo.primaryTable)
                .innerJoin(queryInfo.orderLineTable)
                .on(queryInfo.primaryTable.field(ORDER.ID).eq(queryInfo.orderLineTable.field(ORDER_LINE.ORDER)))
                .innerJoin(queryInfo.productTable)
                .on(queryInfo.orderLineTable.field(ORDER_LINE.PRODUCT).eq(queryInfo.productTable.field(PRODUCT.ID)))

        // Add the joins to the query.
        joinInstanceSets.forEach { set -> set.joinInstances.forEach { it.join(select) } }

        // Add sorting to ensure that all items with same order ID are next to each other.
        // TODO: allow for custom sorting first, limits, etc., for paging.
        val finalQuery = select
                .withConditions(conditionProvider?.invoke(queryInfo))
                .orderBy(queryInfo.primaryTable.field(ORDER.ID))

        return finalQuery.fetchRowsAsync().thenApply {
            getEntities(queryInfo, it, requestInfo?.entityCreationListener, joinInstanceSets)
        }
    }

    /**
     * Gets all the entities from each of the passed in [rows]. Optionally informs the passed in [entityCreationListener]
     * of each of them, if this value is supplied.
     *
     * @param joinInstanceSets The joins that were included in the query, so that joined entities can be found and
     * reported to the listener too.
     */
    private fun getEntities(queryInfo: OrderQueryInfo, rows: List<Row>, entityCreationListener: EntityCreationListener?,
                            joinInstanceSets: List<JoinInstanceSet<out Record>>)
            : List<Order> {

        if (rows.isEmpty())
            return listOf()

        val orders = mutableListOf<Order>()

        fun Row.getOrderId() = queryInfo.getLong(this, queryInfo.primaryTable, ORDER.ID)

        fun createOrder(row: Row, orderLines: List<Order.Line>): Boolean {
            return orders.add(Order(
                    row.getOrderId(),
                    queryInfo.getLong(row, queryInfo.primaryTable, ORDER.CUSTOMER),
                    queryInfo.getDate(row, queryInfo.primaryTable, ORDER.DATE),
                    queryInfo.getString(row, queryInfo.primaryTable, ORDER.DELIVERY_ADDRESS),
                    // Don't take a direct reference of the orderLines: clone it so that changes made to the variable
                    // sent in don't affect this order.
                    orderLines.toList())
                    // Inform the entity creation listener of the order.
                    .also { entityCreationListener?.onEntityCreated(it) })
        }

        val currentOrderLines = mutableListOf<Order.Line>()
        for (i in 0 until rows.size) {
            val currentRow = rows[i]

            if (i > 0) {
                val previousRow = rows[i - 1]
                // This isn't the first one - check in case this is a line from a new order, in which case we can create
                // the order for the previously collected lines.
                if (previousRow.getOrderId() != currentRow.getOrderId()) {
                    // Previous order line is last line in that order: create the order and reset the list of lines.
                    createOrder(previousRow, currentOrderLines)
                    currentOrderLines.clear()
                }
            }

            currentOrderLines.add(with(currentRow.toOrderLineRecord(queryInfo)) {
                Order.Line(this.id, currentRow.toProductRecord(queryInfo, queryInfo.productTable).toEntity(), this
                        .price, currentRow.getOrderId())
            })

            if (entityCreationListener != null) {
                joinInstanceSets
                        .flatMap { it.getJoinedEntities(currentRow, queryInfo, recordProvider, recordToEntityConverterProvider) }
                        .forEach { entityCreationListener.onEntityCreated(it) }
            }
        }
        createOrder(rows.last(), currentOrderLines)

        return orders
    }
}

/**
 * Gets an [OrderLineRecord] from this [Row], reading the data from the passed in [queryInfo]'s
 * [orderLineTable][OrderQueryInfo.orderLineTable].
 *
 * @param queryInfo The object containing the information about the query that produced this row, so that the correct
 * aliased names for tables/fields can be found.
 */
fun Row.toOrderLineRecord(queryInfo: OrderQueryInfo) = this.toOrderLineRecord(queryInfo, queryInfo.orderLineTable)

/**
 * Gets an [OrderLineRecord] from this [Row], reading the data from the passed in [orderLineTable].
 *
 * @param queryInfo The object containing the information about the query that produced this row, so that the correct
 * aliased names for tables/fields can be found.
 * @param orderLineTable: The instance of the table from which the record is to be extracted.
 */
fun Row.toOrderLineRecord(queryInfo: OrderQueryInfo, orderLineTable: Table<OrderLineRecord>): OrderLineRecord {
    return OrderLineRecord(
            queryInfo.getLong(this, orderLineTable, ORDER_LINE.ID),
            queryInfo.getLong(this, orderLineTable, ORDER_LINE.ORDER),
            queryInfo.getLong(this, orderLineTable, ORDER_LINE.PRODUCT),
            queryInfo.getDouble(this, orderLineTable, ORDER_LINE.PRICE)
    )
}

/**
 * Subclass of [QueryInfo] for use by the [OrderRepository], which, in order to create an [Order] object, requires data
 * from multiple tables. Specifically, when constructed this object automatically joins to the [ORDER_LINE] and
 * [PRODUCT] tables. These are exposed by the [orderLineTable] and [productTable] properties respectively.
 */
class OrderQueryInfo(table: Table<OrderRecord>) : QueryInfo<OrderRecord>(table) {
    val orderLineTable: Table<OrderLineRecord> = addJoinedTable(ORDER_LINE, primaryTable, "line", false)
    val productTable: Table<ProductRecord> = addJoinedTable(PRODUCT, orderLineTable, "prod", false)
}