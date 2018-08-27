package com.yg.gqlwfdl.dataaccess

import com.yg.gqlwfdl.services.Entity
import io.reactiverse.pgclient.PgPool
import io.reactiverse.pgclient.Row
import io.reactiverse.pgclient.Tuple
import org.jooq.*
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import kotlin.reflect.KCallable
import kotlin.reflect.full.memberProperties

/**
 * Repository interface for performing basic insert/updates of entities to storage.
 */
interface MutatingRepository<TEntity, TId> {
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
    fun insert(entity: TEntity): CompletionStage<TId>

    /**
     * Inserts a [List] of entities to the repository. Returns a [List] of [CompletionStage] objects which are
     * each resolved when their respective insert is completed.
     */
    fun insert(entities: List<TEntity>): List<CompletionStage<TId>>
}

/**
 * DB implementation of [MutatingRepository] using Jooq for SQL generation and reactive pg client for execution.
 *
 * @param TEntity [Entity] subclass corresponding to the Jooq [TRecord] [TableRecord] subclass
 * @param TId Id type used in [TEntity]. This should correspond to the type of [table.identity.field]
 * @param create A properly initialised Jooq DSL context used to generate the SQL for the current table to be executed
 * @param connectionPool The PgPool connection pool on which to execute the generated queries
 * @param table The Jooq [Table]
 */
open class DBMutatingEntityRepository<TEntity : Entity<TId>, TId, TRecord : TableRecord<TRecord>>(
        protected val create: DSLContext,
        protected val connectionPool: PgPool,
        val table: Table<TRecord>,
        private val tableFieldMapper: EntityPropertyToTableFieldMapper<TEntity, Field<*>> =
                DefaultEntityPropertyToTableFieldMapper()
) : MutatingRepository<TEntity, TId> {
    override fun insert(entity: TEntity): CompletionStage<TId> {
        return insert(listOf(entity)).first()
    }

    override fun insert(entities: List<TEntity>): List<CompletionStage<TId>> {
        // Need to exclude ID field from fieldList as we are not inserting this but retrieving it.
        val fieldList = table.fields().filter { it != table.identity.field }.toList()
        val insertQueryInfo = create
                .insertInto(table)
                .columns(fieldList)

        // Use an array of empty strings for the values of the query at this point. These will be replaced during
        // execution with the proper values from the batch. This is just used to generate the correct query in Jooq.
        // Not specifying values will result in default values being inserted, rather than the query being generated
        // with the proper param placeholder list to be replaced during execution.
        insertQueryInfo.values(List(fieldList.size) { "" })

        // Map database table fields to entity properties.
        val memberProperties = entities.first().javaClass.kotlin.memberProperties
        val bindProperties =  tableFieldMapper.mapEntityPropertiesToTableFields(memberProperties, fieldList)

        // Array of CompletableFuture instances to be resolved in order with the IDs of the inserted entities.
        val futures = Array(size = entities.size) { CompletableFuture<TId>() }

        // Map the entities to a batch of tuples representing each entity. The order of the entity properties added
        // to the tuple is decided by the bind values extracted from the Jooq query object and so will be married up
        // with the query parameters properly at execution.
        val batch = entities.map { entity ->
            val batchTuple = Tuple.tuple()
            // Run through the list of properties mapped by convention to bind values and add the values for these to
            // the tuple to be passed on batch execution. Tuple represents a single entity and a row to be
            // inserted/udpated.
            bindProperties.forEach {
                batchTuple.addValue(it?.invoke(entity))
            }
            batchTuple
        }

        // Modify the current query to return the ID field for the inserted records
        val query = insertQueryInfo.returning(table.identity.field)

        connectionPool.getConnection {
            if (it.failed()) {
                println("Failed to get connection ${it.failed()}")
                throw it.cause()
            }

            val sql = query.getSqlWithNumberedParams(fieldList.size)
            connectionPool.preparedBatch(sql, batch) { asyncResultRowSet ->
                if (asyncResultRowSet.failed()) {
                    println(asyncResultRowSet.cause())
                    throw asyncResultRowSet.cause()
                }

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
        }

        return futures.map {
            it.minimalCompletionStage() as? CompletionStage<TId> ?: throw Exception("Type system is rubbish")
        }.toList()
    }
}
