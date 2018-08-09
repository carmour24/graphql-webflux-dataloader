package com.yg.gqlwfdl.dataaccess.joins

import com.yg.gqlwfdl.childFields
import com.yg.gqlwfdl.dataaccess.db.Tables.COMPANY
import com.yg.gqlwfdl.dataaccess.db.Tables.CUSTOMER
import graphql.language.Field
import org.jooq.Record
import org.jooq.Table
import org.springframework.stereotype.Component

/**
 * Interface used to map GraphQL fields to [JoinRequest] objects, so that, based on what GraphQL fields the client
 * requests, the server can join the necessary tables when querying the database, to get as much data as possible to
 * satisfy the client's requests, with the smallest number of database hits.
 */
interface GraphQLFieldToJoinMapper {
    /**
     * Returns a list of [JoinRequest] objects from the passed in [graphQLField]. These describe the database joins that
     * should be performed in order to have all the data requested by the passed in GraphQL field's children.
     *
     * @param sourceTable The main table being queried, which generally conceptually corresponds to the GraphQL field.
     */
    fun <TRecord : Record> getJoinRequests(graphQLField: Field, sourceTable: Table<TRecord>)
            : List<JoinRequest<out Any, TRecord, out Record>>
}

/**
 * Implementation of [GraphQLFieldToJoinMapper] whose data is based on a list of [GraphQLFieldToJoinMapping] objects.
 * Provides functionality for converting these definitions to the [JoinRequest]s required by the interface. Concrete
 * implementations of this are responsible for supplying the actual mappings.
 */
abstract class DefinitionBasedGraphQLFieldToJoinMapper(private val mappings: List<GraphQLFieldToJoinMapping<out Record>>)
    : GraphQLFieldToJoinMapper {

    /**
     * Gets a list containing all the join mappings whose primary table matches the passed in [table].
     */
    private fun <TRecord : Record> allFrom(table: Table<TRecord>): List<GraphQLFieldToJoinMapping<TRecord>> {
        return mappings.filter { it.sourceTable == table }.map {
            // We can ignore unchecked casts here as the previous check in the filter ensure this wouldn't happen
            @Suppress("UNCHECKED_CAST")
            it as GraphQLFieldToJoinMapping<TRecord>
        }
    }

    override fun <TRecord : Record> getJoinRequests(graphQLField: Field, sourceTable: Table<TRecord>)
            : List<JoinRequest<out Any, TRecord, out Record>> {

        // TODO: the below is OK for many-to-one and one-to-one joins, but one-to-many will mess things up.  Do we need to
        // only define *-to-one joins here?  Or mark joins by what type of relationship they are, and filter to only include
        // the right ones at the right point (e.g. in allFrom...)?

        // For each child field of the passed in field, get its corresponding join definition and, if there is one, ask
        // it to create its join requests. Otherwise just return an empty list.
        val possibleJoins = allFrom(sourceTable)
        return graphQLField.childFields.flatMap {
            possibleJoins.firstOrNull { join -> it.name == join.graphQLFieldName }?.createRequests(it, this) ?: listOf()
        }
    }
}

/**
 * Implementation of [DefinitionBasedGraphQLFieldToJoinMapper] using the joins from the sample database.
 */
@Component
class DefaultGraphQLFieldToJoinMapper : DefinitionBasedGraphQLFieldToJoinMapper(
        GraphQLFieldToJoinMapping.fromJoinDefinitions(CUSTOMER_COMPANY, CUSTOMER_OUT_OF_OFFICE_DELEGATE,
                COMPANY_PRIMARY_CONTACT, PRICING_DETAILS_VAT_RATE, PRICING_DETAILS_DISCOUNT_RATE,
                PRICING_DETAILS_PREFERRED_PAYMENT_METHOD, PRODUCT_COMPANY)
                .plus(listOf(
                        GraphQLFieldToJoinMapping("pricingDetails", CUSTOMER, listOf(
                                NestedJoinDefinition(CUSTOMER_PRICING_DETAILS, listOf(
                                        NestedJoinDefinition(PRICING_DETAILS_VAT_RATE),
                                        NestedJoinDefinition(PRICING_DETAILS_DISCOUNT_RATE),
                                        NestedJoinDefinition(PRICING_DETAILS_PREFERRED_PAYMENT_METHOD))))),
                        GraphQLFieldToJoinMapping("pricingDetails", COMPANY,
                                listOf(NestedJoinDefinition(COMPANY_PRICING_DETAILS, listOf(
                                        NestedJoinDefinition(PRICING_DETAILS_VAT_RATE),
                                        NestedJoinDefinition(PRICING_DETAILS_DISCOUNT_RATE),
                                        NestedJoinDefinition(PRICING_DETAILS_PREFERRED_PAYMENT_METHOD))))))))