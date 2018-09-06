package com.yg.gqlwfdl.dataaccess

import com.opidis.unitofwork.data.ExecutionInfo
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
import java.util.logging.Logger
import kotlin.reflect.KCallable
import kotlin.reflect.KMutableProperty
import kotlin.reflect.full.memberProperties

/**
 * Repository interface for performing basic insert/updates of entities to storage.
 */
interface MutatingRepository<TEntity, TId, TExecutionInfo : ExecutionInfo> {
    fun <TEntity : Entity<*>, TRecord : Record> EntityRepository<*, *>.newRecordFrom(entity: TEntity,
                                                                                     createRecord: KCallable<TRecord>):
            TRecord {
        val record = createRecord.call()
        record.from(entity)
        return record
    }

    /**
     * Inserts a [TEntity] to the repository. Returns a [CompletableFuture] object which is resolved when the insert
     * is completed.
     */
    fun insert(entity: TEntity, executionInfo: TExecutionInfo? = null): CompletionStage<TId>

    /**
     * Inserts a [List] of entities to the repository. Returns a [List] of [CompletionStage] objects which are
     * each resolved when their respective insert is completed.
     */
    fun insert(entities: List<TEntity>, executionInfo: TExecutionInfo? = null): List<CompletionStage<TId>>

    fun update(entity: TEntity, executionInfo: TExecutionInfo? = null): CompletionStage<TEntity>

    fun update(entities: List<TEntity>, executionInfo: TExecutionInfo? = null): List<CompletionStage<TEntity>>
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
    private val logger: Logger? = Logger.getLogger(this.javaClass.name)

    override fun update(entity: TEntity, executionInfo: PgClientExecutionInfo?): CompletionStage<TEntity> = update(listOf(entity), executionInfo).first()

    override fun update(entities: List<TEntity>, executionInfo: PgClientExecutionInfo?): List<CompletionStage<TEntity>> {
        val fieldListNoId = table.fieldsWithoutIdentity()
        val fieldListPlusId = listOf(*fieldListNoId.toTypedArray(), table.identity.field)

        val (updateQueryInfo, batch) = queryForEntities(entities, fieldListPlusId) {
            create
                    .update(table)
                    .set(fieldListNoId.associate { it to "" })
        }

        // Modify the current query to specify the ID for the rows to update and return all fields for the updated
        // records
        val query = updateQueryInfo.where(field("ID").eq("")).returning(fieldListPlusId)

        // Array of CompletableFuture instances to be resolved in order with the updated contents of the entities passed
        // in to be updated. This allows us to ensure that the entities are as expected.
        val futures = Array(size = entities.size) { CompletableFuture<TEntity>() }

        val sql = query.getSqlWithNumberedParams(fieldListPlusId.size)

        logger?.log(Level.FINE, "Generated query string for update: \n$sql")

        // If an execution info has been specified then use the client from that as it may be part of a transaction,
        // otherwise use the constructor injected client, which will perform queries outside of a transaction.
        val client = executionInfo?.pgClient ?: pgClient

        executeQuery(sql, client, batch, { throwable -> futures.forEach { it.completeExceptionally(throwable) } }) { asyncResultRowSet ->
            logger?.log(Level.FINE, "Successfully executed query")

            val mutableEntityList = entities.toMutableList()

            val mappings = tableFieldMapper.mapEntityPropertiesToTableFields(
                    entities.first().javaClass.kotlin.memberProperties,
                    fieldListPlusId
            )

            // Iterate over each result set, there should be one per entity we insert.
            asyncResultRowSet.forEachIndexed { index, result ->
                println("Rowset count = ${result.value().size()}")
                val row = result.value().first()
                val foundIndex = mutableEntityList.indexOfFirst { it.id == row.getValue(table.identity.field.name) }

                logger?.log(Level.FINEST, "index is equal to found index: ${index == foundIndex}")

                val entity = mutableEntityList.removeAt(foundIndex)

                mappings.forEachIndexed { index, property ->
                    val value = row.getValue(fieldListPlusId[index].name)
                    if (property is KMutableProperty<*>) {
                        property.setter.call(entity, value)
                    }
                }

                futures[index].complete(entity)
            }
        }

        return futures.toList()
    }

    override fun insert(entity: TEntity, executionInfo: PgClientExecutionInfo?): CompletionStage<TId> {
        return insert(listOf(entity), executionInfo).first()
    }

    override fun insert(entities: List<TEntity>, executionInfo: PgClientExecutionInfo?): List<CompletionStage<TId>> {
        // Need to exclude ID field from fieldList if we are not inserting it but only retrieving it.
        // However, for this to be the case all entities should have null ID field. It's not possible to mix and
        // match some with ID and some without.
        val fieldList = when {
            entities.all { it.id == null } -> table.fieldsWithoutIdentity()
            entities.none { it.id == null } -> table.fields().toList()
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

        // Modify the current query to return the ID field for the inserted records
        val query = insertQueryInfo.returning(table.identity.field)

        // Array of CompletableFuture instances to be resolved in order with the IDs of the inserted entities.
        val futures = Array(size = entities.size) { CompletableFuture<TId>() }

        val sql = query.getSqlWithNumberedParams(fieldList.size)

        logger?.log(Level.FINE, "Generated query string for insert: \n$sql")

        // If an execution info has been specified then use the client from that as it may be part of a transaction,
        // otherwise use the constructor injected client, which will perform queries outside of a transaction.
        val client = executionInfo?.pgClient ?: pgClient

        executeQuery(sql, client, batch, { throwable ->
            futures.forEachIndexed { index, future ->
                logger?.log(Level.INFO, "Completing exceptionally future $index")
                future.completeExceptionally(throwable)
            }
        }
        ) { asyncResultRowSet ->
            // Iterate over each result set, there should be one per entity we insert.
            asyncResultRowSet.forEachIndexed { index, result ->
                // Within each result set iterate over each row.
                // There should only be one row for an insert.
                // Select the appropriate function to perform completion of promises for the insert of the batch
                // inserted entities. The function is selected by the type of the primary key field. This will
                // not work with composite keys.
                // The completion function is selected to avoid having to recalculate on each entity insertion;
                // currently we only support batch inserts to the same table.
                val completeEntityInsertPromise: (Row) -> Unit = when (table.identity.field.type.kotlin) {
                    Long::class -> { row ->
                        val future = futures[index] as CompletableFuture<Long>
                        future.complete(row.getLong(0))
                    }
                    UUID::class -> { row ->
                        val future = futures[index] as CompletableFuture<UUID>
                        future.complete(row.getUUID(0))
                    }
                    else -> { row ->
                        row.getValue(0) as Any
                    }
                }

                result.forEach { completeEntityInsertPromise(it) }
            }
        }

        return futures.map {
            it.minimalCompletionStage() as? CompletionStage<TId> ?: throw Exception("Type system is rubbish")
        }.toList()
    }

    protected fun executeQuery(
            sql: String,
            client: PgClient,
            batch: List<Tuple>,
            failureAction: (Throwable) -> Unit,
            successAction: (AsyncResult<PgRowSet>) -> Unit
    ) {
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
                batchTuple.addValue(it?.invoke(entity))
            }
            batchTuple
        }
        return (queryInfo to batch)
    }

    // Extension method to retrieve all table fields excluding the identity field. Useful for updates, and inserts
    // when the identity field is already specified.
    private fun Table<*>.fieldsWithoutIdentity() = fields().filter { it != table.identity.field }.toList()
}
