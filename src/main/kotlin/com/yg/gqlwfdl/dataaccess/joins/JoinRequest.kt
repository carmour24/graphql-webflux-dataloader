package com.yg.gqlwfdl.dataaccess.joins

import com.yg.gqlwfdl.dataaccess.QueryInfo
import org.jooq.Record
import org.jooq.Table
import org.jooq.TableField

/**
 * A request for a join to be created when a query is executed.
 *
 * @param TFieldType The type of the fields to be joined.
 * @param TPrimaryRecord The type of the primary record in the join.
 * @param TForeignRecord The type of the foreign record in the join.
 * @property definition The [JoinDefinition] which this request is based on.
 * @property subsequentJoins Any subsequent join from the [TForeignRecord]'s table.
 */
class JoinRequest<TFieldType : Any, TPrimaryRecord : Record, TForeignRecord : Record>(
        private val definition: JoinDefinition<TFieldType, TPrimaryRecord, TForeignRecord>,
        private val subsequentJoins: List<JoinRequest<out Any, TForeignRecord, out Record>>) {

    /**
     * Gets a [JoinInstance] based on this request, using the passed in [primaryTableInstance] as the primary side
     * of the join.
     *
     * @param primaryTableInstance The instance of the primary table to join to.
     * @param queryInfo The object containing information about the query to be executed. Whenever a new table is added
     * to the query, this object is informed.
     */
    fun createInstance(primaryTableInstance: Table<TPrimaryRecord>, queryInfo: QueryInfo<out Record>)
            : JoinInstance<TFieldType, TPrimaryRecord, TForeignRecord> {

        val aliasedForeignTable = queryInfo.addJoinedTable(definition.foreignField.table, primaryTableInstance, definition.name)

        // We can ignore unchecked casts below because we know the items will come as the right types. We need to cast
        // for the generics to be used.
        @Suppress("UNCHECKED_CAST")
        val primaryFieldInstance =
                primaryTableInstance.field(definition.primaryField) as TableField<TPrimaryRecord, TFieldType>
        @Suppress("UNCHECKED_CAST")
        val foreignFieldInstance =
                aliasedForeignTable.field(definition.foreignField) as TableField<TForeignRecord, TFieldType>

        val subsequentJoinInstances = subsequentJoins.map { it.createInstance(aliasedForeignTable, queryInfo) }
        return JoinInstance(definition, primaryFieldInstance, foreignFieldInstance, subsequentJoinInstances)
    }
}