package com.yg.gqlwfdl.dataaccess.joins

import com.yg.gqlwfdl.ClientField
import com.yg.gqlwfdl.dataaccess.db.Tables.COMPANY
import com.yg.gqlwfdl.dataaccess.db.Tables.CUSTOMER
import org.jooq.Record
import org.jooq.Table
import org.springframework.stereotype.Component

/**
 * Interface used to map fields requested by the client (e.g. GraphQL fields) to [JoinRequest] objects, so that, based
 * on what fields the client requests, the server can join the necessary tables when querying the database, to get as
 * much data as possible to satisfy the client's requests, with the smallest number of database hits.
 */
interface ClientFieldToJoinMapper {
    /**
     * Returns a list of [JoinRequest] objects from the passed in [clientFields]. These describe the database joins that
     * should be performed in order to have all the data requested by the passed in client fields.
     *
     * @param sourceTable The main table being queried, which generally conceptually corresponds to the parent of the
     * passed in [clientFields].
     */
    fun <TRecord : Record> getJoinRequests(clientFields: List<ClientField>, sourceTable: Table<TRecord>)
            : List<JoinRequest<out Any, TRecord, out Record>>
}

/**
 * Implementation of [ClientFieldToJoinMapper] whose data is based on a list of [ClientFieldToJoinMapping] objects.
 * Provides functionality for converting these definitions to the [JoinRequest]s required by the interface. Concrete
 * implementations of this are responsible for supplying the actual mappings.
 */
abstract class DefinitionBasedClientFieldToJoinMapper(private val mappings: List<ClientFieldToJoinMapping<out Record>>)
    : ClientFieldToJoinMapper {

    /**
     * Gets a list containing all the join mappings whose primary table matches the passed in [table].
     */
    private fun <TRecord : Record> allFrom(table: Table<TRecord>): List<ClientFieldToJoinMapping<TRecord>> {
        return mappings.filter { it.sourceTable == table }.map {
            // We can ignore unchecked casts here as the previous check in the filter ensure this wouldn't happen
            @Suppress("UNCHECKED_CAST")
            it as ClientFieldToJoinMapping<TRecord>
        }
    }

    override fun <TRecord : Record> getJoinRequests(clientFields: List<ClientField>, sourceTable: Table<TRecord>)
            : List<JoinRequest<out Any, TRecord, out Record>> {

        // TODO: the below is OK for many-to-one and one-to-one joins, but one-to-many will mess things up.  Do we need to
        // only define *-to-one joins here?  Or mark joins by what type of relationship they are, and filter to only include
        // the right ones at the right point (e.g. in allFrom...)?

        // For each child field of the passed in field, get its corresponding join definition and, if there is one, ask
        // it to create its join requests. Otherwise just return an empty list.
        val possibleJoins = allFrom(sourceTable)
        return clientFields.flatMap {
            possibleJoins.firstOrNull { join -> it.name == join.clientFieldName }?.createRequests(it, this) ?: listOf()
        }
    }
}

/**
 * Implementation of [DefinitionBasedClientFieldToJoinMapper] using the joins from the sample database.
 */
@Component
class DefaultClientFieldToJoinMapper : DefinitionBasedClientFieldToJoinMapper(
        ClientFieldToJoinMapping.fromJoinDefinitions(CUSTOMER_COMPANY, CUSTOMER_OUT_OF_OFFICE_DELEGATE,
                COMPANY_PRIMARY_CONTACT, PRICING_DETAILS_VAT_RATE, PRICING_DETAILS_DISCOUNT_RATE,
                PRICING_DETAILS_PREFERRED_PAYMENT_METHOD, PRODUCT_COMPANY)
                .plus(listOf(
                        ClientFieldToJoinMapping("pricingDetails", CUSTOMER, listOf(
                                NestedJoinDefinition(CUSTOMER_PRICING_DETAILS, listOf(
                                        NestedJoinDefinition(PRICING_DETAILS_VAT_RATE),
                                        NestedJoinDefinition(PRICING_DETAILS_DISCOUNT_RATE),
                                        NestedJoinDefinition(PRICING_DETAILS_PREFERRED_PAYMENT_METHOD))))),
                        ClientFieldToJoinMapping("pricingDetails", COMPANY,
                                listOf(NestedJoinDefinition(COMPANY_PRICING_DETAILS, listOf(
                                        NestedJoinDefinition(PRICING_DETAILS_VAT_RATE),
                                        NestedJoinDefinition(PRICING_DETAILS_DISCOUNT_RATE),
                                        NestedJoinDefinition(PRICING_DETAILS_PREFERRED_PAYMENT_METHOD))))))))