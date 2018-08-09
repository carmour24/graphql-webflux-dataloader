package com.yg.gqlwfdl.dataaccess

import com.yg.gqlwfdl.*
import com.yg.gqlwfdl.dataaccess.joins.*
import com.yg.gqlwfdl.services.Entity
import com.yg.gqlwfdl.services.EntityWrapper
import graphql.schema.DataFetchingEnvironment
import io.reactiverse.pgclient.PgPool
import io.reactiverse.pgclient.Row
import org.jooq.*
import org.jooq.conf.ParamType
import reactor.core.publisher.Mono
import reactor.core.publisher.MonoSink
import java.util.concurrent.CompletableFuture
import graphql.language.Field as GraphQLField

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
        protected val create: DSLContext,
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
                env.getJoinRequests(),
                env?.requestContext?.dataLoaderPrimerEntityCreationListener,
                env?.field?.childFields)
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
                env.getJoinRequests(),
                env?.requestContext?.dataLoaderPrimerEntityCreationListener,
                env?.field?.childFields)
    }

    /**
     * Returns a [CompletableFuture] which, when completed, will provide a [List] of all the [TEntity] items in the
     * underlying database table.
     *
     * @param joinRequests The joins that should be added to the query to fetch related items, if any are required.
     * @param entityCreationListener The listener to inform whenever an [Entity] is created. This is done by calling it
     * [EntityCreationListener.onEntityCreated] function.
     * @param graphQLFields A list of the fields requested on the entities being returned, when called from the context
     * of a GraphQL request.
     */
    protected open fun findAll(joinRequests: List<JoinRequest<out Any, TRecord, out Record>>?,
                               entityCreationListener: EntityCreationListener?,
                               graphQLFields: List<GraphQLField>? = null)
            : CompletableFuture<List<TEntity>> {

        return withLogging("querying ${table.name} for all records") {
            find(entityCreationListener, joinRequests, graphQLFields)
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
     * @param graphQLFields A list of the fields requested on the entities being returned, when called from the context
     * of a GraphQL request.
     */
    protected open fun findByIds(ids: List<TId>,
                                 joinRequests: List<JoinRequest<out Any, TRecord, out Record>>?,
                                 entityCreationListener: EntityCreationListener?,
                                 graphQLFields: List<GraphQLField>? = null)
            : CompletableFuture<List<TEntity>> {

        return withLogging("querying ${table.name} for records with IDs $ids") {
            find(entityCreationListener, joinRequests, graphQLFields) {
                listOf(it.primaryTable.field(idField).`in`(ids))
            }
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
    protected open fun getQueryInfo(table: Table<TRecord> = this.table) = QueryInfo(table) as TQueryInfo

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
     * Runs a SELECT query on the passed in [queryInfo]'s [primaryTable][QueryInfo.primaryTable], optionally adding
     * joins, soring, limits. Then uses the passed in [entityProvider] to convert the rows to entities of type [TResult].
     *
     * @param TResult The type of entity being returned.
     * @param entityProvider The function to call to convert a [Row] into an entity of type [TResult].
     * @param queryInfo The object which will store information about the query being built up (e.g. the table/field
     * aliases). If omitted, created by default by calling [getQueryInfo].
     * @param entityCreationListener The object to inform as the entities are created. Defaults to null, in which case
     * it's ignored.
     * @param defaultJoins Any joins to add by default. If omitted, populated by calling [getDefaultJoins].
     * @param joinRequests The joins being requested, e.g. by a GraphQL request.
     * @param conditionProvider The object that will supply any conditions that should be added to the query. If null,
     * no conditions are applied, and every record is returned.
     * @param customJoiner An optional function that can updated the [SelectJoinStep] being built up by adding more joins
     * to it. Used in cases when completely custom join behaviour is required, e.g. joining to nested (derived) tables
     * in a way that can't be handled by the standard [JoinRequest] object model.
     * @param orderBy An optional list of fields to sort by.
     * @param limit An optional limit to the number of rows to return.
     * @return A [CompletableFuture] which will be completed when the query returns its results, and which will expose
     * the results as a [List] of [TResult] objects.
     */
    protected fun <TResult : Entity<out Any>> find(
            entityProvider: (TQueryInfo, Row) -> TResult,
            queryInfo: TQueryInfo = getQueryInfo(),
            entityCreationListener: EntityCreationListener? = null,
            defaultJoins: List<JoinInstance<out Any, TRecord, out Record>>? = getDefaultJoins(queryInfo),
            joinRequests: List<JoinRequest<out Any, TRecord, out Record>>? = null,
            conditionProvider: ((TQueryInfo) -> List<Condition>)? = null,
            customJoiner: ((TQueryInfo, SelectJoinStep<Record>) -> Unit)? = null,
            orderBy: List<OrderField<out Any>>? = null,
            limit: Int? = null)
            : CompletableFuture<List<TResult>> {

        // Get all the join instances required.
        val joinInstances = (defaultJoins ?: listOf()).plus(
                joinRequests?.map { it.createInstance(queryInfo.primaryTable, queryInfo) } ?: listOf())

        // Build up the "SELECT... FROM..." part of the query.
        val select = create
                .select(queryInfo.getAllFields())
                .from(queryInfo.primaryTable)

        // Add the joins to the query.
        customJoiner?.invoke(queryInfo, select)
        joinInstances.forEach { it.join(select) }

        val unorderedQuery = select.withConditions(conditionProvider?.invoke(queryInfo))
        val orderedQuery = orderBy?.letIfAny { unorderedQuery.orderBy(it) } ?: unorderedQuery
        val finalQuery = limit?.let { orderedQuery.limit(it) } ?: orderedQuery

        return finalQuery.fetchRowsAsync().thenApply { rows ->
            // Map the rows to the entities and return them.
            rows.map { row ->
                val entity = entityProvider(queryInfo, row)
                if (entityCreationListener != null) {
                    // We need to inform the listener of all the entities, so inform it of the main entity, then
                    // get all the joined entities and inform it of them too.
                    entityCreationListener.onEntityCreated(entity)

                    if (entity is EntityWrapper<*, *>)
                        entityCreationListener.onEntityCreated(entity.entity)

                    getJoinedEntities(row, queryInfo, joinInstances).forEach {
                        entityCreationListener.onEntityCreated(it)
                    }
                }
                entity
            }
        }
    }

    /**
     * Finds all the records that match the conditions supplied by the passed in [conditionProvider], if any.
     *
     * @param entityCreationListener The listener to inform whenever an [Entity] is found. Ignored if null.
     * @param joinRequests Any joins that should be added to the query. Ignored if null.
     * @param graphQLFields A list of the fields requested on the entities being returned, when called from the context
     * of a GraphQL request.
     * @param conditionProvider The object that will supply any conditions that should be added to the query. If null,
     * no conditions are applied, and every record is returned.
     */
    protected open fun find(entityCreationListener: EntityCreationListener?,
                            joinRequests: List<JoinRequest<out Any, TRecord, out Record>>?,
                            graphQLFields: List<GraphQLField>? = null,
                            conditionProvider: ((TQueryInfo) -> List<Condition>)? = null)
            : CompletableFuture<List<TEntity>> {

        return find(
                entityProvider = this::getEntity,
                entityCreationListener = entityCreationListener,
                joinRequests = joinRequests,
                conditionProvider = conditionProvider
        )
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
                    if (it.succeeded()) sink.success(it.result().toList())
                    else sink.error(Exception("Error running query", it.cause()))
                }
            } catch (e: Exception) {
                sink.error(Exception("Error running query", e))
            }
        }.toFuture()
    }

    /**
     * Gets the join requests from `this` [DataFetchingEnvironment], using this [DBEntityRepository] object's
     * [graphQLFieldToJoinMapper].
     */
    protected fun DataFetchingEnvironment?.getJoinRequests() =
            this?.field?.let { graphQLFieldToJoinMapper.getJoinRequests(it, table) }


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