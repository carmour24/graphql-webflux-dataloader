package com.yg.gqlwfdl.dataaccess

import com.yg.gqlwfdl.services.Entity
import io.reactiverse.pgclient.PgPool
import io.reactiverse.pgclient.Tuple
import org.jooq.*
import java.time.LocalDate
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import kotlin.reflect.KCallable
import kotlin.reflect.KProperty1
import kotlin.reflect.full.memberProperties

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

open class DBMutatingEntityRepository<TEntity : Entity<TId>, TId, TRecord : TableRecord<TRecord>>(
        protected val create: DSLContext,
        protected val connectionPool: PgPool,
        val table: Table<TRecord>
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

//        val insertQueryInfo = create.insertQuery(table)
//        insertQueryInfo.setReturning(table.identity)

        val bindParams = fieldList.map { field ->
            Triple<String, DataType<*>, KProperty1<TEntity, *>?>(field.name,
                    field.dataType,
                    entities.first().javaClass.kotlin.memberProperties.find { it.name == field.name })
        }
        val assignableFrom: (Class<*>, Class<*>) -> Boolean = { instanceClass, clazz ->
            instanceClass.isAssignableFrom(clazz)
        }

        val futures = emptyList<CompletableFuture<*>>().toMutableList()

        val batch = entities.map { entity ->
            val batchTuple = Tuple.tuple()

            bindParams.map { it.second.type to it.third }.forEach { (instanceClass, property) ->
                when {
                    assignableFrom(instanceClass, Int::class.java) -> {
                        val future = CompletableFuture<Int>()
                        futures.add(future)
                        batchTuple.addInteger(property?.invoke(entity) as Int)
                    }
                    assignableFrom(instanceClass, Date::class.java) -> {
                        futures.add(CompletableFuture<LocalDate>())
                        batchTuple.addLocalDate(property?.invoke(entity) as LocalDate)
                    }
                    assignableFrom(instanceClass, Long::class.java) -> {
                        futures.add(CompletableFuture<Long>())
                        batchTuple.addLong(property?.invoke(entity) as Long)
                    }
                    assignableFrom(instanceClass, Double::class.java) -> {
                        futures.add(CompletableFuture<Double>())
                        batchTuple.addDouble(property?.invoke(entity) as Double)
                    }
                    assignableFrom(instanceClass, String::class.java) -> {
                        futures.add(CompletableFuture<String>())
                        batchTuple.addString(property?.invoke(entity) as String)
                    }
                }
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

            connectionPool.preparedBatch(query.sql, batch) { asyncResultRowSet ->
                asyncResultRowSet.result().mapIndexed { index, row ->
                    val instanceClass = table.identity.field.type
                    when {
                        assignableFrom(instanceClass, Long::class.java) -> {

                            val future = futures[index] as? CompletableFuture<Long> ?: throw Exception("Type system is rubbish")
                            future.complete(row.getLong(0))
                        }
                        else -> row.getString(0) as Any
                    }


                }
            }
        }

        return futures.map { it.minimalCompletionStage() as? CompletionStage<TId> ?: throw Exception("Type system is rubbish") }.toList()
    }
}

private fun <T> complete(future: CompletableFuture<T>, value: T) = future.complete(value)
