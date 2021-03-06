package com.yg.gqlwfdl.dataaccess.joins

import graphql.language.Field
import org.jooq.Record
import org.jooq.Table

/**
 * Defines a mapping between a GraphQL field and one or more joins that should be included in a query when that GraphQL
 * field is requested, in order that all the data required by it is available in the result set.
 *
 * @param TPrimaryRecord The type of record representing that main database table from which the joins start. For
 * example, say there's a join from a "customer" table to its related "company" table, this type would be CustomerRecord.
 * @property graphQLFieldName The name of the GraphQL field which this object maps to one or more database joins.
 * @property sourceTable The table (containing records of type [TPrimaryRecord]) from which the joins start. In the
 * example of customers with a related company, this would be the "customer" table.
 * @property joinDefinitions The definitions of joins that are required to get all the data required by the GraphQL
 * field. Each of these join definitions are from the [sourceTable], to some other table. Additionally, each of the
 * joins might have more subsequent joins, specified by [NestedJoinDefinition.subsequentJoins]. In the example of
 * customers with a related company, there would be one join definition, from the "customer" table to the "company"
 * table, and it would have no subsequent joins.
 * @property rootForChildGraphQLFields Each GraphQL field can have child fields. If it does, the system will need to
 * know which of the joined tables to use as the starting point for any subsequent joins. This property specifies that.
 * If omitted, this defaults to the the first item in [joinDefinitions], however this can be explicitly specified as
 * null if there are no subsequent joins that exist. The value specified here should be one of the passed in
 * [joinDefinitions] otherwise no child GraphQL fields will have their joins processed.
 */
class GraphQLFieldToJoinMapping<TPrimaryRecord : Record>(
        val graphQLFieldName: String,
        val sourceTable: Table<TPrimaryRecord>,
        private val joinDefinitions: List<NestedJoinDefinition<TPrimaryRecord, out Record>>,
        private val rootForChildGraphQLFields: NestedJoinDefinition<out Record, out Record>? = joinDefinitions.first()) {

    /**
     * Constructs a new instance of this class based on the passed in join definition. This constructor can be used
     * when a GraphQL field maps directly to a single database join.
     */
    constructor(joinDefinition: JoinDefinition<out Any, TPrimaryRecord, out Record>)
            : this(joinDefinition.name, joinDefinition.primaryField.table, listOf(NestedJoinDefinition(joinDefinition)))

    /**
     * Creates all the [JoinRequest] objects required to have the data necessary to populate a response containing the
     * data requested by the passed in [graphQLField]. Includes the joins defined by this mapping, and, if applicable,
     * each join will in turn be populated with the data necessary for the GraphQL field's children.
     *
     * @param graphQLField The field which corresponded to this mapping, and for which the [JoinRequest]s are needed.
     * @param mapper The object used to map GraphQL fields to database joins, which is required so that the specified
     * [graphQLField]'s children can be mapped to their database joins.
     */
    fun createRequests(graphQLField: Field, mapper: GraphQLFieldToJoinMapper) =
            joinDefinitions.map { it.createRequest(graphQLField, mapper, rootForChildGraphQLFields) }

    companion object {
        /**
         * Converts the passed in (vararg) array of [JoinDefinition]s to a corresponding list of
         * [GraphQLFieldToJoinMapping], by constructing a join mapping from each join definition.
         */
        fun fromJoinDefinitions(vararg joinDefinitions: JoinDefinition<out Any, out Record, out Record>)
                : List<GraphQLFieldToJoinMapping<out Record>> {

            return joinDefinitions.map { GraphQLFieldToJoinMapping(it) }
        }
    }
}