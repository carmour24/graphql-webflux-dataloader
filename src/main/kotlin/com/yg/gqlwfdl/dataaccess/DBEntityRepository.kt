package com.yg.gqlwfdl.dataaccess

import com.yg.gqlwfdl.ClientField
import com.yg.gqlwfdl.dataaccess.DBEntityRepository.JoinRequestSource.Companion.FIELD_PATH_SEPARATOR
import com.yg.gqlwfdl.dataaccess.joins.*
import com.yg.gqlwfdl.letIfAny
import com.yg.gqlwfdl.logMessage
import com.yg.gqlwfdl.services.Entity
import com.yg.gqlwfdl.withLogging
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
 * When [findAll] or [findByIds] are called, the passed in [EntityRequestInfo] (if any) is interrogated to see if any
 * joins need to be added to the generated database queries, based on the requested client fields. Additionally, its
 * [creationListener][EntityRequestInfo.entityCreationListener] is informed of any created entities.
 *
 * @param TEntity The type of the entity to which this repository provides access.
 * @param TId The type of value which defines the unique ID of the entity (typically corresponding to the type of the
 * primary key in the main underlying database table).
 * @param TRecord The [Record] which represents a single entity in the underlying database table. In the case where an
 * entity is based on data from multiple database tables, this should be the primary table (sometimes known as the
 * aggregate root).
 * @param TQueryInfo The type of the [QueryInfo] object which this repository uses. See [getQueryInfo] for details.
 * @property create The DSL context used as the starting point for all operations when working with the JOOQ API. This
 * is automatically injected by spring-boot-starter-jooq.
 * @property connectionPool The database connection pool.
 * @property recordToEntityConverterProvider The object to convert [Record]s to [TEntity] objects.
 * @property clientFieldToJoinMapper The object to use when finding objects in the context of a GraphQL request, to
 * know which joins to add to the database queries, based on the requested GraphQL fields.
 * @property recordProvider An object which can extract data from a [Row] and convert it to the relevant [Record].
 * @property table The database table which this repository is providing access to. In the case where an entity is based on
 * data from multiple database tables, this should be the primary table (sometimes known as the aggregate root).
 * @property idField The field from the table which stores the unique ID of each record, i.e. the primary key field.
 */
abstract class DBEntityRepository<
        TEntity : Entity<TId>, TId : Any, TRecord : Record, TQueryInfo : QueryInfo<TRecord>>(
        protected val create: DSLContext,
        private val connectionPool: PgPool,
        protected val recordToEntityConverterProvider: JoinedRecordToEntityConverterProvider,
        private val clientFieldToJoinMapper: ClientFieldToJoinMapper,
        protected val recordProvider: RecordProvider,
        protected val table: Table<TRecord>,
        private val idField: TableField<TRecord, TId>)
    : EntityRepository<TEntity, TId> {

    /**
     * See [EntityRepository.findAll]. Note that in this implementation the passed in [EntityRequestInfo] (if any) is
     * interrogated to see if any joins need to be added to the generated database queries, based on the requested
     * client fields. Additionally, its [creationListener][EntityRequestInfo.entityCreationListener] is informed of any
     * created entities.
     */
    override fun findAll(requestInfo: EntityRequestInfo?): CompletableFuture<List<TEntity>> {
        return withLogging("querying ${table.name} for all records") {
            find(requestInfo)
        }
    }

    /**
     * See [EntityRepository.findByIds]. Note that in this implementation the passed in [EntityRequestInfo] (if any) is
     * interrogated to see if any joins need to be added to the generated database queries, based on the requested
     * client fields. Additionally, its [creationListener][EntityRequestInfo.entityCreationListener] is informed of any
     * created entities.
     */
    override fun findByIds(ids: List<TId>, requestInfo: EntityRequestInfo?): CompletableFuture<List<TEntity>> {
        return withLogging("querying ${table.name} for records with IDs $ids") {
            find(requestInfo) { listOf(it.primaryTable.field(idField).`in`(ids)) }
        }
    }

    /**
     * Finds all the records that match the conditions supplied by the passed in [conditionProvider], if any.
     *
     * @param requestInfo Information about the request, such as the fields of the entity which were requested by the
     * client, if the call was made from the context of a client request.
     * @param conditionProvider The object that will supply any conditions that should be added to the query. If null,
     * no conditions are applied, and every record is returned.
     */
    protected abstract fun find(requestInfo: EntityRequestInfo? = null,
                                conditionProvider: ((TQueryInfo) -> List<Condition>)? = null)
            : CompletableFuture<List<TEntity>>

    /**
     * Gets an instance of a [TQueryInfo] object to use to store information about a query as it's being built up and
     * executed. The base implementation returns an actual [QueryInfo] object, using this repository's [table] as its
     * [QueryInfo.primaryTable]. However subclasses can override if they need a specific implementation, e.g. one which
     * stores specific joinInstances of joined tables (typically used when a repository works with data based on more than
     * one table).
     */
    // Ignore unsafe cast - we know this is safe.
    @Suppress("UNCHECKED_CAST")
    protected open fun getQueryInfo(table: Table<TRecord> = this.table) = QueryInfo(table) as TQueryInfo

    /**
     * Executes the query specified by the receiver, and returns a [CompletableFuture] which will complete when the
     * query's results are returned.
     */
    protected fun SelectFinalStep<Record>.fetchRowsAsync(): CompletableFuture<List<Row>> {

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
     * Defines a table in a query which can be the source of a join (i.e. the primary side of the join).
     *
     * @param TPrimaryRecord The type of record of the table.
     * @param sourceTableInstance The (typically aliased) instance of the table to use as the primary side of the join.
     * Note that this is the actual instance of the table (typically retrieved from a [QueryInfo] object, which is
     * normally aliased: not the default global "template" instance of the table.
     * @param clientFieldPath The path, in the hierarchy of requested [ClientField]s, to which this table corresponds.
     * Defaults to null, which means that this object corresponds to the root of the hierarchy. However in the case where
     * a single entity corresponds to a hierarchy or client fields (and tables), this can be used to map specific fields
     * to specific tables. For example, say there's an ORDER table and an ORDER_LINE table; the Order entity is based on
     * data from both tables; joins could initiate from either of these tables; and the client field hierarchy is:
     * ```
     * order
     *     # data about the order, from ORDER table
     *     lines
     *         # data about each order line, from the ORDER_LINE table
     * ```
     * In this case there would be two JoinRequestSource objects: the first would have the instance of the ORDER table
     * used in the query, and a null clientFieldPath. The second would have the instance of the ORDER_LINE table used in
     * the query, and the clientFieldPath would have a value of "lines".  If there is then a further child of "lines",
     * e.g. "product", then its clientFieldPath would have a value of "lines/product". The separator is defined in
     * [FIELD_PATH_SEPARATOR]
     */
    protected data class JoinRequestSource<TPrimaryRecord : Record>(
            val sourceTableInstance: Table<TPrimaryRecord>,
            val clientFieldPath: String? = null) {

        /**
         * Finds the [ClientField] from the supplied list of [clientFields] which corresponds to this object's
         * [clientFieldPath].
         */
        fun findClientField(clientFields: List<ClientField>): ClientField? {
            // Split the path by a slash to find all the parts, then loop round each part looking for a child of that name.

            /**
             * Finds the [ClientField] in `this` which matches the passed in path, expressed as an array of [pathParts].
             */
            fun List<ClientField>.findByPath(pathParts: List<String>): ClientField? {
                val pathPart = pathParts.firstOrNull() ?: return null
                val clientField = this.firstOrNull { it.name == pathPart } ?: return null
                return if (pathParts.size == 1) clientField else clientField.children.findByPath(pathParts.drop(1))
            }

            return if (clientFieldPath == null) null
            else clientFields.findByPath(clientFieldPath.trim(FIELD_PATH_SEPARATOR).split(FIELD_PATH_SEPARATOR))
        }

        companion object {
            /**
             * The separator to use when building up a path of client fields to map a table to. See [clientFieldPath] for
             * more info.
             */
            const val FIELD_PATH_SEPARATOR = '/'
        }
    }

    /**
     * A set of [JoinInstance]s, all of which come from the same source table ([sourceTableInstance]), i.e. have the same
     * table as the primary side of the join.
     *
     * @param TPrimaryRecord The type of the [Record] in the [sourceTableInstance].
     */
    protected class JoinInstanceSet<TPrimaryRecord : Record>(
            private val sourceTableInstance: Table<TPrimaryRecord>,
            val joinInstances: List<JoinInstance<out Any, TPrimaryRecord, out Record>>) {

        /**
         * Gets all the joined entities available from the passed in [row], for the joins which start from this object's
         * [sourceTableInstance] (including any subsequent joins of those joins).
         *
         * @param row A single result row from a query
         * @param queryInfo The information about the query which is to be executed. Used to get the correct aliases for
         * the tables/fields in the query.
         * @param recordProvider The object to use to read the data in the [row] and convert it to [Record]s of the
         * required types.
         * @param recordToEntityConverterProvider The object to use to convert [Record]s to [Entity] objects.
         */
        fun getJoinedEntities(row: Row,
                              queryInfo: QueryInfo<out Record>,
                              recordProvider: RecordProvider,
                              recordToEntityConverterProvider: JoinedRecordToEntityConverterProvider)
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

            return joinInstances.letIfAny {
                // There is at least one join instance: all these (top-level) join instances all come from the source
                // table this set is working with, so get that record: it is the primary record for all these initial joins.
                @Suppress("UNCHECKED_CAST") // We know this cast is safe as sourceTable is Table<TPrimaryRecord>
                val primaryRecord = recordProvider.getRecord(queryInfo, row, sourceTableInstance,
                        sourceTableInstance.recordType as Class<TPrimaryRecord>)

                // If there's no primary record return null (which will be converted to an empty list by the elvis
                // operator below). Otherwise go round each join and get its corresponding result (if the join resulted
                // in data). Each join result might itself have subsequent join results.
                if (primaryRecord == null) null
                else joinInstances.mapNotNull { instance ->
                    // If there is a value in the join's primary field instance create the primary record from it then
                    // use it to get the join's result, if any.
                    if (row.getValue(queryInfo.getAliasedFieldName(instance.primaryFieldInstance)) == null) null
                    else instance.getResult(row, primaryRecord, queryInfo, recordProvider)
                }.getAllEntities(primaryRecord)
            } ?: listOf()
        }
    }

    /**
     * Gets the [JoinInstanceSet] (a set of [JoinInstance]s) which come from the passed in [joinRequestSource]'s
     * [sourceTableInstance][JoinRequestSource.sourceTableInstance], based on the client fields defined in `this`
     * [EntityRequestInfo]'s [childFields][EntityRequestInfo.childFields]. Uses this [DBEntityRepository] object's
     * [clientFieldToJoinMapper] to map the client fields to their corresponding joins.
     */
    protected fun <TPrimaryRecord : Record> EntityRequestInfo.getJoinInstanceSet(
            joinRequestSource: JoinRequestSource<TPrimaryRecord>, queryInfo: TQueryInfo)
            : JoinInstanceSet<TPrimaryRecord> {

        // Find the child fields whose parent matches the passed in join request's client field path.
        val childFields = if (joinRequestSource.clientFieldPath == null) childFields
        else joinRequestSource.findClientField(childFields)?.children

        // For each of the child fields, find the matching join requests.
        val joinRequests = childFields?.letIfAny {
            clientFieldToJoinMapper.getJoinRequests(it, joinRequestSource.sourceTableInstance)
        } ?: listOf()

        return JoinInstanceSet(joinRequestSource.sourceTableInstance,
                joinRequests.map { it.createInstance(joinRequestSource.sourceTableInstance, queryInfo) })
    }

    /**
     * Gets the [JoinInstanceSet]s defining the joins to include when servicing a request for `this` [EntityRequestInfo]
     * object (i.e. in order to get as much data as possible to populate the response for the requested
     * [childFields][EntityRequestInfo.childFields]). The base implementation defaults to returning a list with just one
     * join instance set, using the passed in [queryInfo]'s [primaryTable][QueryInfo.primaryTable]. However subclasses
     * can override and add more, if working with more than one table from which joins can issue.
     */
    protected open fun EntityRequestInfo.getJoinInstanceSets(queryInfo: TQueryInfo): List<JoinInstanceSet<out Record>> =
            listOf(getJoinInstanceSet(JoinRequestSource(queryInfo.primaryTable), queryInfo))

    /**
     * Updates the receiver and adds to it the passed in [conditions], if any. Returns the same instance that was passed in.
     */
    protected fun SelectJoinStep<Record>.withConditions(conditions: List<Condition>?): SelectConnectByStep<Record> =
            conditions?.letIfAny { where(it) } ?: this

    // Below is the WIP old code for working with Fluxes rather than CompletableFuture<List<T>>, in case we want to go
    // back to that approach at some point.

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