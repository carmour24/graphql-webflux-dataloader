package com.yg.gqlwfdl.dataaccess

import com.yg.gqlwfdl.services.Entity
import io.reactiverse.pgclient.PgPool
import io.reactiverse.pgclient.Tuple
import org.jooq.*
import org.jooq.conf.ParamType
import java.time.LocalDate
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import kotlin.reflect.KCallable
import kotlin.reflect.KClass
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

        for (entity in entities) {
            //  Try to use an array of empty strings for the values as we'll be replacing those.
            insertQueryInfo.values(List(fieldList.size) { "" })
        }

        // Map fields to properties. Using a mutable list and removing the found entries so on each iteration of
        // searching for a field we are only searching through properties which have not previously been identified.
        // TODO: Maybe something like associateby would be more performant/readable.
        val unidentifiedEntityProperties = entities.first().javaClass.kotlin.memberProperties.toMutableList()
        val bindParams = fieldList.map { field ->
            val normalizedFieldName = field.name.replace("_", "").toLowerCase()
            val property = unidentifiedEntityProperties.find {
                // TODO: Properly map properties to fields somewhere, this mapping of ignoring case and removing
                // _ from field names is OK in the interim but needs a reasonable solution.
                val normalizedPropName = it.name.toLowerCase()
                val found = (normalizedPropName == normalizedFieldName)
                        .or(normalizedPropName.endsWith("id") && normalizedPropName.substring(0,
                                normalizedPropName.length - 2) == normalizedFieldName)
                found
            }

            unidentifiedEntityProperties.remove(property)

            Triple<String, DataType<*>, KProperty1<TEntity, *>?>(field.name, field.dataType, property)
        }

        val assignableFrom: (Class<*>, KClass<*>) -> Boolean = { instanceClass, clazz ->
            instanceClass.kotlin == clazz
        }

        val futures = Array(size = entities.size) {
            CompletableFuture<TId>()
        }

        val batch = entities.map { entity ->
            val batchTuple = Tuple.tuple()

            bindParams.map { it.second.type to it.third }.forEach { (instanceClass, property) ->
                when {
                    assignableFrom(instanceClass, Int::class) -> {
                        batchTuple.addInteger(property?.invoke(entity) as Int?)
                    }
                    assignableFrom(instanceClass, Date::class) -> {
                        batchTuple.addLocalDate(property?.invoke(entity) as LocalDate?)
                    }
                    assignableFrom(instanceClass, Long::class) -> {
                        batchTuple.addLong(property?.invoke(entity) as Long?)
                    }
                    assignableFrom(instanceClass, Double::class) -> {
                        batchTuple.addDouble(property?.invoke(entity) as Double?)
                    }
                    assignableFrom(instanceClass, String::class) -> {
                        batchTuple.addString(property?.invoke(entity) as String?)
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

            val sql=query.getSqlWithNumberedParams(fieldList.size)
            connectionPool.preparedBatch(sql, batch) { asyncResultRowSet ->
                if (asyncResultRowSet.failed()) {
                    println(asyncResultRowSet.cause())
                    throw asyncResultRowSet.cause()
                }

                val result = asyncResultRowSet.result()
                result.forEachIndexed { index, row ->
                    println("Completing futures[$index]")
                    when(table.identity.field.type.kotlin)  {
                        Long::class -> {

                            val future = futures[index] as CompletableFuture<Long>
                            val indexValue = row.getLong(0)
                            println("completing a long: $indexValue")
                            future.complete(indexValue)
                        }
                        UUID::class -> {
                            val future = futures[index] as CompletableFuture<UUID>
                            future.complete(row.getUUID(0))
                        }
                        else -> row.getString(0) as Any
                    }
                }
            }
        }

        return futures.map {
            it.minimalCompletionStage() as? CompletionStage<TId> ?: throw Exception("Type system is rubbish")
        }.toList()
    }
}

// TODO: Replace with tested version from Unit Of Work project.
fun Query.getSqlWithNumberedParams(fieldCount: Int): String {
    var index = 0
    return this.getSQL(ParamType.NAMED).replace(Regex("(?:\\:(\\d+))")) {
        "\$${(index++ % fieldCount) + 1}"
    }
}
