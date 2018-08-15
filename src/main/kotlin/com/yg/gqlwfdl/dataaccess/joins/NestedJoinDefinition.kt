package com.yg.gqlwfdl.dataaccess.joins

import com.yg.gqlwfdl.ClientField
import org.jooq.Record

/**
 * Represents a [JoinDefinition], along with any subsequent joins (from the [JoinDefinition.foreignField] to one or more
 * other tables. This then ends up defining a hierarchy of join definitions, which can be treated as a single set, e.g.
 * when mapping GraphQL fields to database joins.
 *
 * @property joinDefinition The main join definition.
 * @property subsequentJoins Any subsequent joins from the [JoinDefinition.foreignField] to one or more other tables. If
 * omitted, an empty list is used, meaning there are no subsequent joins.
 */
data class NestedJoinDefinition<TPrimaryRecord : Record, TForeignRecord : Record>(
        private val joinDefinition: JoinDefinition<out Any, TPrimaryRecord, TForeignRecord>,
        private val subsequentJoins: List<NestedJoinDefinition<TForeignRecord, out Record>> = listOf()) {

    /**
     * Creates a [JoinRequest] instance based on this [NestedJoinDefinition].
     *
     * @param childFields The child client fields to be used to populate the [subsequent joins][JoinRequest.subsequentJoins]
     * of the join request.
     * @param mapper The mapper to use to get the subsequent joins for the passed in [childFields].
     * @param rootForChildFields Each client field can have child fields. If it does, the system will need to know which
     * of the joined tables to use as the starting point for any subsequent joins. This parameter specifies that. The
     * value specified here should either be this [NestedJoinDefinition] itself, or one of its
     * [subsequentJoins][NestedJoinDefinition.subsequentJoins]. This can be null if there are no subsequent joins that
     * need to be processed.
     */
    fun createRequest(childFields: List<ClientField>,
                      mapper: ClientFieldToJoinMapper,
                      rootForChildFields: NestedJoinDefinition<out Record, out Record>?)
            : JoinRequest<out Any, TPrimaryRecord, out Record> {

        val subsequentJoinRequests = subsequentJoins.map {
            it.createRequest(childFields, mapper, rootForChildFields)
        }

        return if (this === rootForChildFields)
            joinDefinition.createRequest(childFields, mapper, subsequentJoinRequests)
        else
            JoinRequest(joinDefinition, subsequentJoinRequests)
    }
}