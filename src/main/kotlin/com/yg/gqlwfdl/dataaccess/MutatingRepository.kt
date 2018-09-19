package com.yg.gqlwfdl.dataaccess

import com.opidis.unitofwork.data.ExecutionInfo
import com.yg.gqlwfdl.getLogger
import com.yg.gqlwfdl.services.Entity
import io.reactiverse.pgclient.*
import io.reactiverse.pgclient.Row
import io.vertx.core.AsyncResult
import org.jooq.*
import org.jooq.impl.DSL.field
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.logging.Level
import kotlin.reflect.full.memberProperties

/**
 * Repository interface for performing basic insert/updates of entities to storage.
 */
interface MutatingRepository<TEntity, TId, TExecutionInfo : ExecutionInfo> {
    /**
     * Inserts a [TEntity] to the repository. Returns a [CompletableFuture] object which is resolved when the insert
     * is completed.
     */
    fun insert(entity: TEntity, executionInfo: TExecutionInfo? = null): CompletionStage<TId>

    /**
     * Inserts a [List] of entities to the repository. Returns a [List] of [CompletionStage] objects which are
     * each resolved when their respective insert is completed.
     */
    fun insert(entities: List<TEntity>, executionInfo: TExecutionInfo? = null): CompletionStage<List<TId>>

    fun update(entity: TEntity, executionInfo: TExecutionInfo? = null): CompletionStage<Int>

    fun update(entities: List<TEntity>, executionInfo: TExecutionInfo? = null): CompletionStage<IntArray>

    fun delete(entities: List<TEntity>, executionInfo: TExecutionInfo?): CompletionStage<IntArray>
}

class PgClientExecutionInfo(val pgClient: PgClient) : ExecutionInfo

/**
 * DB implementation of [MutatingRepository] using Jooq for SQL generation and reactive pg client for execution.
 *
 * @param TEntity [Entity] subclass corresponding to the Jooq [TRecord] [TableRecord] subclass
 * @param TId Id type used in [TEntity]. This should correspond to the type of [table.identity.field]
 * @param create A properly initialised Jooq DSL context used to generate the SQL for the current table to be executed
 * @param pgClient The PgPool connection pool on which to execute the generated queries
 * @param table The Jooq [Table]
 */
open class DBMutatingEntityRepository<TEntity : Entity<TId>, TId, TRecord : TableRecord<TRecord>>(
        protected val create: DSLContext,
        protected val pgClient: PgClient,
        val table: Table<TRecord>,
        private val tableFieldMapper: EntityPropertyToTableFieldMapper<TEntity, Field<*>> =
                DefaultEntityPropertyToTableFieldMapper()
) : MutatingRepository<TEntity, TId, PgClientExecutionInfo> {
    private val logger = getLogger()

    override fun update(entity: TEntity, executionInfo: PgClientExecutionInfo?): CompletionStage<Int> = update(listOf(entity), executionInfo).thenApply { it.first() }

    override fun update(entities: List<TEntity>, executionInfo: PgClientExecutionInfo?): CompletionStage<IntArray> {
        val fieldListNoId = table.fieldsWithoutIdentity()
        val fieldListPlusId = listOf(*fieldListNoId.toTypedArray(), table.identity.field)

        val (updateQueryInfo, batch) = queryForEntities(entities, fieldListPlusId) {
            create
                    .update(table)
                    .set(fieldListNoId.associate { it to "" })
        }

        // Modify the current query to specify the ID for the rows to update and return all fields for the updated
        // records
        val query = updateQueryInfo.where(field("ID").eq(""))//.returning(fieldListPlusId)

        // Array of CompletableFuture instances to be resolved in order with the updated contents of the entities passed
        // in to be updated. This allows us to ensure that the entities are as expected.
        val future = CompletableFuture<IntArray>()

        val sql = query.getSqlWithNumberedParams(fieldListPlusId.size)

        logger?.log(Level.FINE, "Generated query string for update: \n$sql")

        executeQuery(sql, executionInfo, batch, makeFailureAction(future)) { asyncResultRowSet ->
            logger?.log(Level.FINE, "Successfully executed query")

            val resultList = emptyList<Int>().toMutableList()
            // Iterate over each result set finding the count of entities affected.

            asyncResultRowSet.forEachRowSet {
                resultList.add(it.updatedCount())
            }

            future.complete(resultList.toIntArray())
        }

        return future
    }

    override fun insert(entity: TEntity, executionInfo: PgClientExecutionInfo?): CompletionStage<TId> {
        return insert(listOf(entity), executionInfo).thenApply { it.first() }
    }

    override fun insert(entities: List<TEntity>, executionInfo: PgClientExecutionInfo?): CompletionStage<List<TId>> {
        // Need to exclude ID field from fieldList if we are not inserting it but only retrieving it.
        // However, for this to be the case all entities should have null ID field. It's not possible to mix and
        // match some with ID and some without.
        val (fieldList, entityIdsNotSpecified) = when {
            entities.all { it.id == null } -> table.fieldsWithoutIdentity() to true
            entities.none { it.id == null } -> table.fields().toList() to false
            else -> throw Exception("Inserted entities must either all have IDs or none should have IDs")
        }

        val (insertQueryInfo, batch) = queryForEntities(entities, fieldList = fieldList) { fieldList ->
            val insertQueryInfo = create
                    .insertInto(table)
                    .columns(fieldList)

            // Use an array of empty strings for the values of the query at this point. These will be replaced during
            // execution with the proper values from the batch. This is just used to generate the correct query in Jooq.
            // Not specifying values will result in default values being inserted, rather than the query being generated
            // with the proper param placeholder list to be replaced during execution.
            insertQueryInfo.values(List(fieldList.size) { "" })
        }

        val query = if (entityIdsNotSpecified) {
            // Modify the current query to return the ID field for the inserted records
            insertQueryInfo.returning(table.identity.field)
        } else {
            insertQueryInfo.returning()
        }

        // Future to be completed with the list of IDs returned from the newly inserted entities.
        val future = CompletableFuture<List<TId>>()


        val sql = query.getSqlWithNumberedParams(fieldList.size)

        logger?.log(Level.FINE, "Generated query string for insert: \n$sql")

        // If an execution info has been specified then use the client from that as it may be part of a transaction,
        // otherwise use the constructor injected client, which will perform queries outside of a transaction.
        val getRowIdentityValue = getRowIdentityValueFunc()

        // Select the appropriate function to perform completion of promises for the insert of the batch
        // inserted entities. The function is selected by the type of the primary key field. This will
        // not work with composite keys.
        // The completion function is selected to avoid having to recalculate on each entity insertion;
        // currently we only support batch inserts to the same table.


        executeQuery(sql, executionInfo, batch, makeFailureAction(future)) { asyncResultRowSet ->
            val rows = asyncResultRowSet.flattenRows()
            val ids = rows.map(getRowIdentityValue)

            // If the entity was not specified during insert then we should populate the entities with it.
            if (entityIdsNotSpecified) {
                ids.forEachIndexed { index, id ->
                    entities[index].id = id
                }
            }

            future.complete(ids)
        }
        /*) { asyncResultRowSet ->
            // Iterate over each result set, there should be one per entity we insert.
            val result = asyncResultRowSet.map { result ->
                // Within each result set iterate over each row.
                // There should only be one row for an insert.
                result.map { getRowIdentityValue(it) }.toList()
            }.result()

            // If the entity was not specified during insert then we should populate the entities with it.
            if (entityIdsNotSpecified) {
                result.forEachIndexed {index, tId ->
                    entities[index].id = tId
                }
            }

            asyncResultRowSet.forEachIndexed()

            future.complete(result)
        }
        */

        return future
    }

    override fun delete(entities: List<TEntity>, executionInfo: PgClientExecutionInfo?): CompletionStage<IntArray> {
        val ids = entities.map { Tuple.of(it.id) }

        val sql = create
                .delete(table)
                .where(table.identity.field.`in`(ids))
                .getSqlWithNumberedParams(1)

        val future = CompletableFuture<IntArray>()
        executeQuery(sql, executionInfo, ids, makeFailureAction(future)) { asyncResultRowSet ->
            val listOfUpdateCounts = asyncResultRowSet.mapRowSet {
                it.updatedCount()
            }

            future.complete(listOfUpdateCounts.toIntArray())
        }

        return future
    }

    private fun getRowIdentityValueFunc(): (Row) -> TId {
        return when (table.identity.field.type.kotlin) {
            Long::class -> { row ->
                row.getLong(0) as TId
            }
            UUID::class -> { row ->
                row.getUUID(0) as TId
            }
            else -> { row ->
                row.getValue(0) as TId
            }
        }
    }

    protected fun executeQuery(
            sql: String,
            executionInfo: PgClientExecutionInfo?,
            batch: List<Tuple>,
            failureAction: (Throwable) -> Unit,
            successAction: (AsyncResult<PgRowSet>) -> Unit
    ) {
        // If an execution info has been specified then use the client from that as it may be part of a transaction,
        // otherwise use the constructor injected client, which will perform queries outside of a transaction.
        val client = executionInfo?.pgClient ?: pgClient

        client.preparedBatch(sql, batch) { asyncResultRowSet ->
            if (asyncResultRowSet.failed()) {
                logger?.log(Level.SEVERE, "Failed to execute query ${asyncResultRowSet.cause()}")
                failureAction(asyncResultRowSet.cause())
                throw asyncResultRowSet.cause()
            }

            successAction(asyncResultRowSet)
        }
    }

    protected fun <TQuery : Query> queryForEntities(entities: List<TEntity>, fieldList: List<Field<*>>, createQuery: (List<Field<*>>) -> TQuery): Pair<TQuery, List<Tuple>> {
        val queryInfo = createQuery(fieldList)

        // Map database table fields to entity properties.
        val memberProperties = entities.first().javaClass.kotlin.memberProperties
        val bindProperties = tableFieldMapper.mapEntityPropertiesToTableFields(memberProperties, fieldList)


        // Map the entities to a batch of tuples representing each entity. The order of the entity properties added
        // to the tuple is decided by the bind values extracted from the Jooq query object and so will be married up
        // with the query parameters properly at execution.
        val batch = entities.map { entity ->
            val batchTuple = Tuple.tuple()
            // Run through the list of properties mapped by convention to bind values and add the values for these to
            // the tuple to be passed on batch execution. Tuple represents a single entity and a row to be
            // inserted/updated.
            bindProperties.forEach {
                val propertyValue = it?.invoke(entity)
                if (propertyValue is Entity<*>) {
                    batchTuple.addValue(propertyValue.id)
                } else {
                    batchTuple.addValue(propertyValue)
                }
            }
            batchTuple
        }
        return (queryInfo to batch)
    }

    private fun makeFailureAction(future: CompletableFuture<*>): (Throwable) -> Unit {
        return { throwable ->
            logger?.log(Level.INFO, "Query failed with exception ${throwable.cause}")
            future.completeExceptionally(throwable)
        }
    }


    // Extension method to retrieve all table fields excluding the identity field. Useful for updates, and inserts
// when the identity field is already specified.
    private fun Table<*>.fieldsWithoutIdentity() = fields().filter { it != table.identity.field }.toList()

    private fun AsyncResult<PgRowSet>.forEachRowSet(handler: (PgRowSet) -> Unit) {
        // Iterate over each result set finding the count of entities affected.

        var result = this.result()
        while (result != null) {
            handler(result)
            result = result.next()
        }
    }

    private fun <T> AsyncResult<PgRowSet>.mapRowSet(mapper: (PgRowSet) -> T): List<T> {
        val list = emptyList<T>().toMutableList()

        forEachRowSet { list.add(mapper(it)) }

        return list
    }

}

//fun <TEntity : Entity<*>, TRecord : Record> MutatingRepository<*, *, *>.newRecordFrom(entity: TEntity,
//                                                                                 createRecord: KCallable<TRecord>):
//        TRecord {
//    val record = createRecord.call()
//    record.from(entity)
//    return record
//}

