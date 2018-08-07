package com.yg.gqlwfdl.dataaccess

import com.yg.gqlwfdl.*
import com.yg.gqlwfdl.dataaccess.joins.*
import com.yg.gqlwfdl.services.Entity
import graphql.schema.DataFetchingEnvironment
import io.reactiverse.pgclient.PgPool
import io.reactiverse.pgclient.Row
import org.jooq.*
import org.jooq.conf.ParamType
import reactor.core.publisher.Mono
import reactor.core.publisher.MonoSink
import java.util.concurrent.CompletableFuture

/**
 * A repository providing access to an entity (aka domain model object) (of type [TEntity]), by querying one or more
 * database tables.
 *
 * When [findAll] or [findByIds] are called, the passed in [DataFetchingEnvironment] (if any) is interrogated to see if
 * any joins need to be added to the generated database queries, based on the requested GraphQL fields. Additionally,
 * the [DataFetchingEnvironment.requestContext]'s [RequestContext.dataLoaderPrimerEntityCreationListener] is informed
 * of any created entities, unless a specific [EntityCreationListener] is passed in instead.
 *
 * @param TEntity The type of the entity to which this repository provides access.
 * @param TId The type of value which defines the unique ID of the entity (typically corresponding to the type of the
 * primary key in the main underlying database table).
 * @param TRecord The [UpdatableRecord] which represents a single record in the underlying database table. In the case
 * where an entity is based on data from multiple database tables, this should be the primary table (sometimes known
 * as the aggregate root).
 * @param TQueryInfo The type of the [QueryInfo] object which this repository uses. See [getQueryInfo] for details.
 * @property create The DSL context used as the starting point for all operations when working with the JOOQ API. This
 * is automatically injected by spring-boot-starter-jooq.
 * @property connectionPool The database connection pool.
 * @property recordToEntityConverterProvider The object to convert [Record]s to [TEntity] objects.
 * @property graphQLFieldToJoinMapper The object to use when finding objects in the context of a GraphQL request, to
 * know which joins to add to the database queries, based on the requested GraphQL fields.
 * @property recordProvider An object which can extract data from a [Row] and convert it to the relevant [Record].
 * @property table The database table which this repository is providing access to. In the case where an entity is based on
 * data from multiple database tables, this should be the primary table (sometimes known as the aggregate root).
 * @property idField The field from the table which stores the unique ID of each record, i.e. the primary key field.
 */
abstract class DBEntityRepository<
        TEntity : Entity<TId>, TId : Any, TRecord : UpdatableRecord<TRecord>, TQueryInfo : QueryInfo<TRecord>>(
        private val create: DSLContext,
        private val connectionPool: PgPool,
        private val recordToEntityConverterProvider: JoinedRecordToEntityConverterProvider,
        private val graphQLFieldToJoinMapper: GraphQLFieldToJoinMapper,
        private val recordProvider: RecordProvider,
        private val table: Table<TRecord>,
        private val idField: TableField<TRecord, TId>)
    : EntityRepository<TEntity, TId> {

    /**
     * See [EntityRepository.findAll]. Note that in this implementation the passed in [DataFetchingEnvironment] (if any)
     * is interrogated to see if any joins need to be added to the generated database queries, based on the requested
     * GraphQL fields. Additionally, the [DataFetchingEnvironment.requestContext]'s
     * [RequestContext.dataLoaderPrimerEntityCreationListener] is informed of any created entities.
     */
    override fun findAll(env: DataFetchingEnvironment?): CompletableFuture<List<TEntity>> {
        return findAll(
                env?.field?.let { graphQLFieldToJoinMapper.getJoinRequests(it, table) },
                env?.requestContext?.dataLoaderPrimerEntityCreationListener)
    }

    /**
     * See [EntityRepository.findByIds]. Note that in this implementation the passed in [DataFetchingEnvironment] (if any)
     * is interrogated to see if any joins need to be added to the generated database queries, based on the requested
     * GraphQL fields. Additionally, the [DataFetchingEnvironment.requestContext]'s
     * [RequestContext.dataLoaderPrimerEntityCreationListener] is informed of any created entities.
     */
    override fun findByIds(ids: List<TId>, env: DataFetchingEnvironment?): CompletableFuture<List<TEntity>> {
        return findByIds(
                ids,
                env?.field?.let { graphQLFieldToJoinMapper.getJoinRequests(it, table) },
                env?.requestContext?.dataLoaderPrimerEntityCreationListener)
    }

    /**
     * Returns a [CompletableFuture] which, when completed, will provide a [List] of all the [TEntity] items in the
     * underlying database table.
     *
     * @param joinRequests The joins that should be added to the query to fetch related items, if any are required.
     * @param entityCreationListener The listener to inform whenever an [Entity] is created. This is done by calling it
     * [EntityCreationListener.onEntityCreated] function.
     */
    protected open fun findAll(joinRequests: List<JoinRequest<out Any, TRecord, out Record>>?,
                               entityCreationListener: EntityCreationListener?)
            : CompletableFuture<List<TEntity>> {

        return withLogging("querying ${table.name} for all records") {
            find(entityCreationListener, joinRequests)
        }
    }

    /**
     * Returns a [CompletableFuture] which, when completed, will provide a [List] of all the [TEntity] items which have
     * the passed in IDs, in the underlying database table.
     *
     * @param ids The IDs of the items to be found.
     * @param joinRequests The joins that should be added to the query to fetch related items, if any are required.
     * @param entityCreationListener The listener to inform whenever an [Entity] is created. This is done by calling it
     * [EntityCreationListener.onEntityCreated] function.
     */
    protected open fun findByIds(ids: List<TId>,
                                 joinRequests: List<JoinRequest<out Any, TRecord, out Record>>?,
                                 entityCreationListener: EntityCreationListener?)
            : CompletableFuture<List<TEntity>> {

        return withLogging("querying ${table.name} for records with IDs $ids") {
            find(entityCreationListener, joinRequests) { listOf(it.primaryTable.field(idField).`in`(ids)) }
        }
    }

    /**
     * Gets an instance of a [TQueryInfo] object to use to store information about a query as it's being built up and
     * executed. The base implementation returns an actual [QueryInfo] object, using this repository's [table] as its
     * [QueryInfo.primaryTable]. However subclasses can override if they need a specific implementation, e.g. one which
     * stores specific instances of joined tables (typically used when a repository works with data based on more than
     * one table).
     */
    // Ignore unsafe cast - we know this is safe.
    @Suppress("UNCHECKED_CAST")
    protected open fun getQueryInfo(table: Table<TRecord>) = QueryInfo(this.table) as TQueryInfo

    /**
     * Gets the main record (i.e. a row from the main table this repository is working with), from the passed in [row]
     *
     * @param queryInfo The information about the query from which the entity is to be retrieved, used for example to
     * get at the aliased fields' names.
     * @param row A row containing all the data in a single result row from a query generated by this repository
     * when finding the items it's working with.
     */
    protected abstract fun getRecord(queryInfo: TQueryInfo, row: Row): TRecord

    /**
     * Creates the entity which this repository provides access to from the passed in [row].
     *
     * @param queryInfo The information about the query from which the entity is to be retrieved, used for example to
     * get at the aliased fields' names.
     * @param row A row containing all the data in a single result row from a query generated by this repository
     * when finding the items it's working with.
     */
    protected abstract fun getEntity(queryInfo: TQueryInfo, row: Row): TEntity

    /**
     * Gets a list of all the [Field]s which exist in the receiver, which is a list of [JoinInstance]s.
     *
     * @param queryInfo The information about the query which is to be executed. Used to get the instances of the
     * aliased fields.
     */
    private fun List<JoinInstance<out Any, out Record, out Record>>.getAllAliasedFields(queryInfo: TQueryInfo)
            : List<Field<out Any>> {

        // For each join in this list ...
        return this.flatMap {
            // ... get all the fields from the foreign side of the join and alias them ...
            queryInfo.getAliasedFields(it.foreignFieldInstance.table)
                    // ... then add all the joins from each of the subsequent joins (if any).
                    .plus(it.subsequentJoins.getAllAliasedFields(queryInfo))
        }
    }

    /**
     * Finds all the records that match the conditions supplied by the passed in [conditionProvider], if any.
     *
     * @param entityCreationListener The listener to inform whenever an [Entity] is found. Ignored if null.
     * @param joinRequests Any joins that should be added to the query. Ignored if null.
     * @param conditionProvider The object that will supply any conditions that should be added to the query. If null,
     * no conditions are applied, and every record is returned.
     */
    private fun find(entityCreationListener: EntityCreationListener?,
                     joinRequests: List<JoinRequest<out Any, TRecord, out Record>>?,
                     conditionProvider: ((TQueryInfo) -> List<Condition>)? = null)
            : CompletableFuture<List<TEntity>> {

        // Get the TQueryInfo object that will build up information about all the tables/fields/aliases in the query
        // so that data can be correctly retrieved from the right fields.
        val queryInfo = getQueryInfo(table)

        // Get all the join instances required.
        val joinInstances = getDefaultJoins(queryInfo).plus(
                joinRequests?.map { it.createInstance(queryInfo.primaryTable, queryInfo) } ?: listOf())

        // Build up the "SELECT... FROM..." part of the query.
        val select = create
                .select(queryInfo.getAliasedFields(queryInfo.primaryTable).plus(joinInstances.getAllAliasedFields(queryInfo)))
                .from(queryInfo.primaryTable)

        // Add the joins to the query.
        joinInstances.forEach { it.join(select) }

        return select
                // Add the WHERE part of the clause.
                .withConditions(conditionProvider?.invoke(queryInfo))
                // Fetch the rows asynchronously.
                .fetchRowsAsync()
                .thenApply { rows ->
                    // Map the rows to the entities and return them.
                    rows.map { row ->
                        val entity = getEntity(queryInfo, row)
                        if (entityCreationListener != null) {
                            // We need to inform the listener of all the entities, so inform it of the main entity, then
                            // get all the joined entities and inform it of them too.
                            entityCreationListener.onEntityCreated(entity)
                            getJoinedEntities(row, queryInfo, joinInstances).forEach {
                                entityCreationListener.onEntityCreated(it)
                            }
                        }
                        entity
                    }
                }

    }

    /**
     * Gets the joins which are needed by default by this repository (i.e. regardless of what joins are requested by a
     * GraphQL request.
     *
     * The base implementation does nothing, which is the desired behaviour for repositories which work with a single
     * table. However subclasses which work with multiple tables should override this and supply the required joins.
     */
    protected open fun getDefaultJoins(queryInfo: TQueryInfo) = listOf<JoinInstance<out Any, TRecord, out Record>>()

    /**
     * Gets all the joined entities available from the passed in [row].
     *
     * @param row A single result row from a query
     * @param queryInfo The information about the query which is to be executed. Used to get the instances of the
     * aliased fields.
     * @param joinInstances The [JoinInstance]s that were included in the query, so that the joined data can be checked
     * to see if it resulted in any more entities being available.
     */
    private fun getJoinedEntities(row: Row,
                                  queryInfo: TQueryInfo,
                                  joinInstances: List<JoinInstance<out Any, TRecord, out Record>>)
            : List<Entity<out Any>> {

        /**
         * Extension method on a list of [JoinResult]s, which gets all entities from a given [primaryRecord], and any
         * subsequent joins in any of the join results.
         */
        fun List<JoinResult<out Any, out Record, out Record>>.getAllEntities(primaryRecord: Record): List<Entity<out Any>> {
            return recordToEntityConverterProvider.converters.flatMap { converter ->
                // First get the entities created from the initial joins.
                converter.getEntities(primaryRecord, this)
            }.plus(
                    // Now check each join's subsequent joins, recursively.
                    this.flatMap { it.subsequentJoins.getAllEntities(it.foreignRecord) }
            )
        }

        // First get a list of the join results, i.e. the join instances which had data on the foreign side of the join.
        // This list contains the top-level joins (i.e. the first level of joins from this repository's main table to
        // its related tables). Each of those join results might in turn have subsequent joins, to create a nested
        // hierarchy.
        return joinInstances.letIfAny {
            // There is at least one join instance: all these (top-level) join instances all come from the main table
            // this repository is working with, so get that record: it is the primary record for all these initial joins.
            val primaryRecord = getRecord(queryInfo, row)

            // Go round the join instances and, for each one, get its corresponding result (if the join resulted in
            // data). Each join result might itself have subsequent join results.
            joinInstances.mapNotNull { joinInstance ->
                // If there is a value in the join's primary field instance create the primary record from it then use it
                // to get the join's result, if any.
                if (row.getValue(queryInfo.getAliasedFieldName(joinInstance.primaryFieldInstance)) == null) null
                else joinInstance.getResult(row, primaryRecord, queryInfo, recordProvider)
            }.getAllEntities(primaryRecord)
        } ?: listOf()
    }

    /**
     * Executes the query specified by the receiver, and returns a [CompletableFuture] which will complete when the
     * query's results are returned.
     */
    private fun SelectFinalStep<Record>.fetchRowsAsync(): CompletableFuture<List<Row>> {

        return Mono.create { sink: MonoSink<List<Row>> ->
            try {
                val sql = getSQL(ParamType.INLINED)
                logMessage("Executing query: $sql")
                connectionPool.query(sql) {
                    if (it.succeeded()) sink.success(it.result().toList()) else throw it.cause()
                }
            } catch (e: Exception) {
                sink.error(Exception("Error running query", e))
            }
        }.toFuture()
    }

    // Below is the code for working with Fluxes rather than CompletableFuture<List<T>>...

    /*

    private fun SelectFinalStep<Record>.fetchResultsAsFlux(queryInfo: TQueryInfo, rowListener: (Row, TEntity) -> Unit)
            : Flux<TEntity> {
        logMessage("Creating flux")
        return Flux.create<TEntity> { sink ->
            sink.onRequest {
                logMessage("Sink request received")
                sink.ifOk<PgConnection>({ connectionPool.getConnection(it) }) { connection ->
                    logMessage("Got DB connection")
                    val sqlWithParams = getSQL(ParamType.INLINED)
                    logMessage("Preparing query: $sqlWithParams")

                    sink.ifOk<PgPreparedQuery>(
                            { connection.prepare(sqlWithParams, it) }, { connection.close() }) { preparedQuery ->
                        logMessage("Got prepared query")

                        connection.begin()

                        logMessage("Creating stream")
                        Note: hard-coded value (50) below...
                        val stream = preparedQuery.createStream(50, Tuple.tuple())
                        stream.handler {
                            NOTE: add error handling here
                            try {
                                val entity = getEntity(queryInfo, it)
                                rowListener(it, entity)
                                sink.next(entity)
                            } catch (e: Exception) {
                                sink.error(e)
                            }
                        }
                        stream.exceptionHandler {
                            logMessage("Stream error: ${it.cause}")
                            sink.error(it)
                        }
                        stream.endHandler {
                            connection.close()
                            logMessage("Stream ended")
                            sink.complete()
                        }
                    }
                }
            }
        }
    }

    fun FluxSink<out Any>.safeExecute(block: () -> Unit) {
        this.safeExecute({}, block)
    }

    fun FluxSink<out Any>.safeExecute(failureHandler: () -> Unit, block: () -> Unit) {
        try {
            block()
        } catch (e: Exception) {
            failureHandler()
            error(e)
        }
    }

    fun <T> FluxSink<out Any>.ifOk(block: (Handler<AsyncResult<T>>) -> Unit, okHandler: (T) -> Unit) =
            this.ifOk(block, {}, okHandler)

    fun <T> FluxSink<out Any>.ifOk(
            block: (Handler<AsyncResult<T>>) -> Unit, failureHandler: () -> Unit, okHandler: (T) -> Unit) {
        safeExecute(failureHandler) {
            block(Handler {
                if (it.failed()) {
                    failureHandler()
                    error(it.cause())
                } else safeExecute(failureHandler) { okHandler(it.result()) }
            })
        }
    }

    */
}

/**
 * Updates the receiver and adds to it the passed in [conditions], if any. Returns the same instance that was passed in.
 */
private fun SelectJoinStep<Record>.withConditions(conditions: List<Condition>?): SelectConnectByStep<Record> =
        conditions?.letIfAny { this.where(it) } ?: this