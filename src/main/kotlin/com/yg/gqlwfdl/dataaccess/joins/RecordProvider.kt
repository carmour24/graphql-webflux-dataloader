package com.yg.gqlwfdl.dataaccess.joins

import com.yg.gqlwfdl.dataaccess.*
import com.yg.gqlwfdl.dataaccess.db.tables.records.*
import io.reactiverse.pgclient.Row
import org.jooq.Record
import org.jooq.Table
import org.springframework.stereotype.Component

/**
 * Used to read data from a row returned from a database query, and convert it to a [Record] of a given type (i.e. a row
 * of data from a particular table in the query).
 */
interface RecordProvider {
    /**
     * Reads the data in the passed in [row], which came from a query whose information is stored in the passed in
     * [queryInfo], and returns the data that row contains from the passed in [table], as a [TRecord]. Returns null if
     * there is no such data (e.g. it was a table joined using an outer join, and there's no data in it).
     *
     * @param queryInfo The [QueryInfo] object containing the information about the query that led to the row being
     * created, i.e. the names of all the aliased fields/tables.
     * @param row The [Row] of data from the query, containing information from all the joined tables.
     * @param table The [Table] from which the record is being sought.
     * @param recordClass The type of the [TRecord] requested. Required so that the type information can be accessed at
     * runtime (it's lost owing to type erasure, hence the need for the actual instance of the class to be passed in).
     */
    fun <TRecord : Record> getRecord(
            queryInfo: QueryInfo<out Record>, row: Row, table: Table<TRecord>, recordClass: Class<TRecord>): TRecord?
}

/**
 * The default implementation of [RecordProvider], which is aware of all the different types of [Record]s the system
 * knows about, and how to get an instance of each one, by calling the relevant repository.
 */
@Component
class DefaultRecordProvider : RecordProvider {
    /**
     * A map of all the functions that can create a [Record] of a given type, from a [Row]. The map is keyed on the
     * class of the record, and hte value is a function which can return that record, given the following three values:
     * * A [QueryInfo] object containing the information about the query that led to the row being created, i.e. the
     * names of all the aliased fields/tables.
     * * The [Row] that contains the data from the query.
     * * The [Table] from which the record is being sought.
     */
    // Ignore unchecked casts here as we know that we will only access a given creator with the correct type of table.
    @Suppress("UNCHECKED_CAST")
    private val recordInstantiators: Map<Class<out Record>, (QueryInfo<out Record>, Row, Table<out Record>) -> Record> =
            mapOf(
                    Pair(CompanyRecord::class.java, { queryInfo, row, table ->
                        row.toCompanyRecord(queryInfo, table as Table<CompanyRecord>)
                    }),
                    Pair(CustomerRecord::class.java, { queryInfo, row, table ->
                        row.toCustomerRecord(queryInfo, table as Table<CustomerRecord>)
                    }),
                    Pair(CompanyPartnershipRecord::class.java, { queryInfo, row, table ->
                        row.toCompanyPartnershipRecord(queryInfo, table as Table<CompanyPartnershipRecord>)
                    }),
                    Pair(PricingDetailsRecord::class.java, { queryInfo, row, table ->
                        row.toPricingDetailsRecord(queryInfo, table as Table<PricingDetailsRecord>)
                    }),
                    Pair(VatRateRecord::class.java, { queryInfo, row, table ->
                        row.toVatRateRecord(queryInfo, table as Table<VatRateRecord>)
                    }),
                    Pair(DiscountRateRecord::class.java, { queryInfo, row, table ->
                        row.toDiscountRateRecord(queryInfo, table as Table<DiscountRateRecord>)
                    }),
                    Pair(PaymentMethodRecord::class.java, { queryInfo, row, table ->
                        row.toPaymentMethodRecord(queryInfo, table as Table<PaymentMethodRecord>)
                    }))

    // Ignore unsafe cast - we know this is safe.
    @Suppress("UNCHECKED_CAST")
    override fun <TRecord : Record> getRecord(queryInfo: QueryInfo<out Record>,
                                              row: Row, table: Table<TRecord>,
                                              recordClass: Class<TRecord>) =
            recordInstantiators[recordClass]?.invoke(queryInfo, row, table) as TRecord?
}