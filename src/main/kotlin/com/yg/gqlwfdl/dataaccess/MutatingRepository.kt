package com.yg.gqlwfdl.dataaccess

import com.yg.gqlwfdl.dataaccess.db.tables.records.CustomerRecord
import com.yg.gqlwfdl.services.Entity
import io.reactiverse.pgclient.PgPool
import io.reactiverse.pgclient.Tuple
import org.jooq.*
import java.time.LocalDate
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import kotlin.reflect.KCallable
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
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

    fun getNextIds(count: Int = 1): TId
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
        val insertQueryInfo = create.insertQuery(table)
        insertQueryInfo.setReturning(table.identity)

        val bindParams = insertQueryInfo.params.map { param ->
            Triple<String, DataType<*>, KProperty1<TEntity, *>?>(param.key,
                    param.value.dataType,
                    entities.first().javaClass.kotlin.memberProperties.find { it.name == param.key })
        }
        val assignableFrom: (Class<*>, Class<*>) -> Boolean = { instanceClass, clazz ->
            instanceClass.isAssignableFrom(clazz)
        }

        val futures = emptyList<Pair<CompletableFuture<*>, KCallable<*>>>().toMutableList()

        val batch = entities.map { entity ->
            val batchTuple = Tuple.tuple()

            bindParams.map { it.second.type to it.third }.forEach { (instanceClass, property) ->
                when {
                    assignableFrom(instanceClass, Int::class.java) -> {
                        val future = CompletableFuture<Int>()
                        futures.add(Pair(future, (i{ future.complete() }))
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

        connectionPool.getConnection {
            if (it.failed()) {
                println("Failed to get connection ${it.failed()}")
                throw it.cause()
            }

            connectionPool.preparedBatch(insertQueryInfo.sql, batch) { asyncResultRowSet ->
                asyncResultRowSet.result().mapIndexed { index, row ->
                    val instanceClass = table.identity.field.type
                    val returnedId = when {
                        assignableFrom(instanceClass, Long::class.java) -> futures[index].complete(row.getLong(0))
                        else -> row.getString(0) as Any
                    }


                }
            }
        }

        return futures
    }

    override fun getNextIds(count: Int): TId {
    }
}