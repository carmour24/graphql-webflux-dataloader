package com.yg.gqlwfdl.dataaccess

import com.yg.gqlwfdl.dataaccess.db.Tables.ORDER_LINE
import com.yg.gqlwfdl.dataaccess.db.Tables.PRODUCT
import com.yg.gqlwfdl.dataaccess.db.tables.records.ProductRecord
import com.yg.gqlwfdl.dataaccess.joins.ClientFieldToJoinMapper
import com.yg.gqlwfdl.dataaccess.joins.JoinedRecordToEntityConverterProvider
import com.yg.gqlwfdl.dataaccess.joins.RecordProvider
import com.yg.gqlwfdl.services.Product
import com.yg.gqlwfdl.services.ProductOrderCount
import io.reactiverse.pgclient.PgPool
import io.reactiverse.pgclient.Row
import org.jooq.*
import org.jooq.impl.DSL.count
import org.springframework.stereotype.Repository
import java.util.concurrent.CompletableFuture

/**
 * Repository providing access to product information.
 */
interface ProductRepository : EntityRepository<Product, Long> {
    /**
     * Returns a [CompletableFuture] which, when completed, will provide a [List] of all [Product] objects with the
     * passed in IDs, along with their order counts, wrapped in an [ProductOrderCount] object.
     *
     * @param requestInfo Information about the request, such as the fields of the entity which were requested by the
     * client, if the call was made from the context of a client request.
     */
    fun findWithOrderCount(ids: List<Long>, requestInfo: EntityRequestInfo? = null):
            CompletableFuture<List<ProductOrderCount>>

    /**
     * Gets the top [count] best selling products.
     *
     * @param requestInfo Information about the request, such as the fields of the entity which were requested by the
     * client, if the call was made from the context of a client request.
     */
    fun findTopSelling(count: Int, requestInfo: EntityRequestInfo? = null): CompletableFuture<List<ProductOrderCount>>
}

/**
 * Concrete implementation of [ProductRepository], which uses a database for its data.
 */
@Repository
class DBProductRepository(create: DSLContext,
                          connectionPool: PgPool,
                          recordToEntityConverterProvider: JoinedRecordToEntityConverterProvider,
                          clientFieldToJoinMapper: ClientFieldToJoinMapper,
                          recordProvider: RecordProvider)
    : DBEntityRepository<Product, Long, ProductRecord, QueryInfo<ProductRecord>>(
        create, connectionPool, recordToEntityConverterProvider, clientFieldToJoinMapper, recordProvider,
        PRODUCT, PRODUCT.ID),
        ProductRepository {

    override fun findWithOrderCount(ids: List<Long>, requestInfo: EntityRequestInfo?) =
            findWithOrderCount(requestInfo) { listOf(it.primaryTable.field(PRODUCT.ID).`in`(ids)) }

    override fun getRecord(queryInfo: QueryInfo<ProductRecord>, row: Row) = row.toProductRecord(queryInfo)

    override fun getEntity(queryInfo: QueryInfo<ProductRecord>, row: Row) = getRecord(queryInfo, row).toEntity()

    override fun find(requestInfo: EntityRequestInfo?,
                      conditionProvider: ((QueryInfo<ProductRecord>) -> List<Condition>)?)
            : CompletableFuture<List<Product>> {

        // Request included the "orderCount" GraphQL field: this means that we need to join to a nested SQL query
        // which will count the number of orders for each product. Call the findWithOrderCount, which will return
        // the products with their order counts, and will cause the order counts (ProductOrderCount)
        // to be cached with the data loader so that the ProductResolver has access to the values later. And before
        // returning the values, map them back to the Product entities themselves.
        return if (requestInfo?.containsField("orderCount") == true)
            findWithOrderCount(requestInfo, null, null, conditionProvider)
                    .thenApply { results -> results.map { it.entity } }
        else
        // Can just use base class's behaviour.
            super.find(requestInfo, conditionProvider)
    }

    override fun findTopSelling(count: Int, requestInfo: EntityRequestInfo?) =
            findWithOrderCount(requestInfo, SortOrder.DESC, count)

    /**
     * Runs a query for products, including the count of orders for each one. Uses [PRODUCT] as the main table, and
     * joins to a nested table which is a SELECT which joins [PRODUCT] to [ORDER_LINE], returning only the product ID
     * and counting the orders.
     *
     * Returns a [CompletableFuture] which will complete when the query returns results, and exposes a [List] of
     * [ProductOrderCount] objects.
     */
    private fun findWithOrderCount(requestInfo: EntityRequestInfo? = null,
                                   orderCountSortOrder: SortOrder? = null,
                                   limit: Int? = null,
                                   conditionProvider: ((QueryInfo<ProductRecord>) -> List<Condition>)? = null)
            : CompletableFuture<List<ProductOrderCount>> {

        val queryInfo = getQueryInfo()
        val orderCountField = count().`as`("order_count")
        val productsWithOrderCount = queryInfo.addJoinedTable(
                create
                        .select(PRODUCT.ID, orderCountField)
                        .from(PRODUCT).innerJoin(ORDER_LINE).on(PRODUCT.ID.eq(ORDER_LINE.PRODUCT))
                        .groupBy(PRODUCT.ID)
                        .asTable("products_with_orders"),
                queryInfo.primaryTable, "orderCount", false)

        return find(
                entityProvider = { qi, row ->
                    ProductOrderCount(getEntity(qi, row), qi.getInt(row, productsWithOrderCount, orderCountField))
                },
                queryInfo = queryInfo,
                entityCreationListener = requestInfo?.creationListener,
                joinRequests = requestInfo?.getJoinRequests(),
                conditionProvider = conditionProvider,
                customJoiner = { qi, select ->
                    select.innerJoin(productsWithOrderCount)
                            .on(qi.primaryTable.field(PRODUCT.ID).eq(productsWithOrderCount.field(PRODUCT.ID)))
                },
                orderBy = if (orderCountSortOrder == null) null else listOf(
                        if (orderCountSortOrder == SortOrder.DESC) orderCountField.desc() else orderCountField.asc()),
                limit = limit
        )
    }
}

/**
 * Converts a [ProductRecord] to its corresponding entity, a [Product].
 */
fun ProductRecord.toEntity() = Product(this.id, this.description, this.price, this.company)

/**
 * Gets a [ProductRecord] from this [Row], reading the data from the passed in [queryInfo]'s
 * [primaryTable][QueryInfo.primaryTable].
 *
 * @param queryInfo The object containing the information about the query that produced this row, so that the correct
 * aliased names for tables/fields can be found.
 */
fun Row.toProductRecord(queryInfo: QueryInfo<ProductRecord>) =
        this.toProductRecord(queryInfo, queryInfo.primaryTable)

/**
 * Gets a [ProductRecord] from this [Row], reading the data from the passed in [productTable].
 *
 * @param queryInfo The object containing the information about the query that produced this row, so that the correct
 * aliased names for tables/fields can be found.
 * @param productTable: The instance of the table from which the record is to be extracted.
 */
fun Row.toProductRecord(queryInfo: QueryInfo<out Record>,
                        productTable: Table<ProductRecord>): ProductRecord {

    return ProductRecord(
            queryInfo.getLong(this, productTable, PRODUCT.ID),
            queryInfo.getString(this, productTable, PRODUCT.DESCRIPTION),
            queryInfo.getDouble(this, productTable, PRODUCT.PRICE),
            queryInfo.getLong(this, productTable, PRODUCT.COMPANY))
}