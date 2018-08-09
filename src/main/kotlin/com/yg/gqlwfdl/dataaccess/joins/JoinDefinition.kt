package com.yg.gqlwfdl.dataaccess.joins

import com.yg.gqlwfdl.ClientField
import org.jooq.Record
import org.jooq.TableField

/**
 * Defines a join that exists in the underlying database.
 *
 * @param TFieldType The type of the fields being joined.
 * @param TPrimaryRecord The type of the primary record in the join.
 * @param TForeignRecord The type of the foreign record in the join.
 * @property name The name of this join. Might match the name of the field in the GraphQL schema which exposes a value
 * which has to be populated by querying a secondary table (although doesn't have to, as a single GraphQL field might
 * require multiple joins to get the required data to populate a response to it).
 * @property primaryField The primary field in the join.
 * @property foreignField The foreign field in the join.
 */
data class JoinDefinition<TFieldType : Any, TPrimaryRecord : Record, TForeignRecord : Record>(
        val name: String,
        val primaryField: TableField<TPrimaryRecord, TFieldType>,
        val foreignField: TableField<TForeignRecord, TFieldType>) {

    /**
     * Creates a [JoinRequest] instance based on this [JoinDefinition].
     *
     * @param childFields The child client fields to be used to populate the [subsequent joins][JoinRequest.subsequentJoins]
     * of the join request.
     * @param mapper The mapper to use to get the subsequent joins for the passed in [childFields].
     * @param subsequentJoinRequests Any subsequent joins that should be added to this, in addition to any which are
     * calculated based on the passed in [childFields].
     */
    fun createRequest(childFields: List<ClientField>,
                      mapper: ClientFieldToJoinMapper,
                      subsequentJoinRequests: List<JoinRequest<out Any, TForeignRecord, out Record>> = listOf())
            : JoinRequest<TFieldType, TPrimaryRecord, TForeignRecord> {
        return JoinRequest(this, mapper.getJoinRequests(childFields, foreignField.table)
                .plus(subsequentJoinRequests))
    }
}