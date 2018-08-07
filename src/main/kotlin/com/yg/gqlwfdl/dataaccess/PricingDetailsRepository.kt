package com.yg.gqlwfdl.dataaccess

import com.yg.gqlwfdl.dataaccess.db.Tables.*
import com.yg.gqlwfdl.dataaccess.db.tables.records.DiscountRateRecord
import com.yg.gqlwfdl.dataaccess.db.tables.records.PaymentMethodRecord
import com.yg.gqlwfdl.dataaccess.db.tables.records.PricingDetailsRecord
import com.yg.gqlwfdl.dataaccess.db.tables.records.VatRateRecord
import com.yg.gqlwfdl.dataaccess.joins.*
import com.yg.gqlwfdl.services.DiscountRate
import com.yg.gqlwfdl.services.PaymentMethod
import com.yg.gqlwfdl.services.PricingDetails
import com.yg.gqlwfdl.services.VatRate
import io.reactiverse.pgclient.PgPool
import io.reactiverse.pgclient.Row
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.Table
import org.jooq.TableField
import org.springframework.stereotype.Repository

/**
 * Repository providing access to pricing details information.
 */
interface PricingDetailsRepository : EntityRepository<PricingDetails, Long>

/**
 * Concrete implementation of [PricingDetailsRepository], which uses a database for its data.
 */
@Repository
class DBPricingDetailsRepository(create: DSLContext,
                                 connectionPool: PgPool,
                                 recordToEntityConverterProvider: JoinedRecordToEntityConverterProvider,
                                 graphQLFieldToJoinMapper: GraphQLFieldToJoinMapper,
                                 recordProvider: RecordProvider)
    : DBEntityRepository<PricingDetails, Long, PricingDetailsRecord, PricingDetailsQueryInfo>(
        create, connectionPool, recordToEntityConverterProvider, graphQLFieldToJoinMapper, recordProvider,
        PRICING_DETAILS, PRICING_DETAILS.ID),
        PricingDetailsRepository {

    override fun getQueryInfo(table: Table<PricingDetailsRecord>) = PricingDetailsQueryInfo(table)

    override fun getRecord(queryInfo: PricingDetailsQueryInfo, row: Row) = row.toPricingDetailsRecord(queryInfo)

    override fun getEntity(queryInfo: PricingDetailsQueryInfo, row: Row): PricingDetails {
        return PricingDetailsRecords(
                row.toPricingDetailsRecord(queryInfo),
                row.toVatRateRecord(queryInfo),
                row.toDiscountRateRecord(queryInfo),
                row.toPaymentMethodRecord(queryInfo)
        ).toEntity()
    }

    // Ignore unchecked casts here for the places we ask for a field's instance from a table, as we know the returned
    // value will be a TableField
    @Suppress("UNCHECKED_CAST")
    override fun getDefaultJoins(queryInfo: PricingDetailsQueryInfo)
            : List<JoinInstance<out Any, PricingDetailsRecord, out Record>> {

        return listOf(
                JoinInstance(
                        PRICING_DETAILS_VAT_RATE,
                        queryInfo.primaryTable.field(PRICING_DETAILS.VAT_RATE) as TableField<PricingDetailsRecord, Long>,
                        queryInfo.vatRateTable.field(VAT_RATE.ID) as TableField<VatRateRecord, Long>),
                JoinInstance(
                        PRICING_DETAILS_DISCOUNT_RATE,
                        queryInfo.primaryTable.field(PRICING_DETAILS.DISCOUNT_RATE) as TableField<PricingDetailsRecord, Long>,
                        queryInfo.discountRateTable.field(DISCOUNT_RATE.ID) as TableField<DiscountRateRecord, Long>),
                JoinInstance(
                        PRICING_DETAILS_PREFERRED_PAYMENT_METHOD,
                        queryInfo.primaryTable.field(PRICING_DETAILS.PREFERRED_PAYMENT_METHOD) as TableField<PricingDetailsRecord, Long>,
                        queryInfo.paymentMethodTable.field(PAYMENT_METHOD.ID) as TableField<PaymentMethodRecord, Long>))
    }
}

/**
 * A class aggregating all the different types of records that contain the data required to construct a [PricingDetails]
 * object.
 */
data class PricingDetailsRecords(val pricingDetailsRecord: PricingDetailsRecord,
                                 val vatRateRecord: VatRateRecord,
                                 val discountRateRecord: DiscountRateRecord,
                                 val paymentMethodRecord: PaymentMethodRecord) {

    /**
     * Converts this [PricingDetailsRecords] object to its corresponding entity, a [PricingDetails] object.
     */
    fun toEntity() = PricingDetails(
            pricingDetailsRecord.id, pricingDetailsRecord.description,
            with(vatRateRecord) { VatRate(id, description, value) },
            with(discountRateRecord) { DiscountRate(id, description, value) },
            with(paymentMethodRecord) { PaymentMethod(id, description, charge) })
}

/**
 * Gets a [VatRateRecord] from this [Row], reading the data from the passed in [queryInfo]'s
 * [vatRateTable][PricingDetailsQueryInfo.vatRateTable].
 *
 * @param queryInfo The object containing the information about the query that produced this row, so that the correct
 * aliased names for tables/fields can be found.
 */
fun Row.toVatRateRecord(queryInfo: PricingDetailsQueryInfo) =
        this.toVatRateRecord(queryInfo, queryInfo.vatRateTable)

/**
 * Gets a [VatRateRecord] from this [Row], reading the data from the passed in [vatRateTable].
 *
 * @param queryInfo The object containing the information about the query that produced this row, so that the correct
 * aliased names for tables/fields can be found.
 * @param vatRateTable: The instance of the table from which the record is to be extracted.
 */
fun Row.toVatRateRecord(queryInfo: QueryInfo<out Record>,
                        vatRateTable: Table<VatRateRecord>)
        : VatRateRecord {

    return VatRateRecord(
            queryInfo.getLong(this, vatRateTable, VAT_RATE.ID),
            queryInfo.getString(this, vatRateTable, VAT_RATE.DESCRIPTION),
            queryInfo.getDouble(this, vatRateTable, VAT_RATE.VALUE))
}

/**
 * Gets a [DiscountRateRecord] from this [Row], reading the data from the passed in [queryInfo]'s
 * [discountRateTable][PricingDetailsQueryInfo.discountRateTable].
 *
 * @param queryInfo The object containing the information about the query that produced this row, so that the correct
 * aliased names for tables/fields can be found.
 */
fun Row.toDiscountRateRecord(queryInfo: PricingDetailsQueryInfo) =
        this.toDiscountRateRecord(queryInfo, queryInfo.discountRateTable)

/**
 * Gets a [DiscountRateRecord] from this [Row], reading the data from the passed in [discountRateTable].
 *
 * @param queryInfo The object containing the information about the query that produced this row, so that the correct
 * aliased names for tables/fields can be found.
 * @param discountRateTable: The instance of the table from which the record is to be extracted.
 */
fun Row.toDiscountRateRecord(queryInfo: QueryInfo<out Record>,
                             discountRateTable: Table<DiscountRateRecord>)
        : DiscountRateRecord {

    return DiscountRateRecord(
            queryInfo.getLong(this, discountRateTable, DISCOUNT_RATE.ID),
            queryInfo.getString(this, discountRateTable, DISCOUNT_RATE.DESCRIPTION),
            queryInfo.getDouble(this, discountRateTable, DISCOUNT_RATE.VALUE))
}

/**
 * Gets a [PaymentMethodRecord] from this [Row], reading the data from the passed in [queryInfo]'s
 * [paymentMethodTable][PricingDetailsQueryInfo.paymentMethodTable].
 *
 * @param queryInfo The object containing the information about the query that produced this row, so that the correct
 * aliased names for tables/fields can be found.
 */
fun Row.toPaymentMethodRecord(queryInfo: PricingDetailsQueryInfo) =
        this.toPaymentMethodRecord(queryInfo, queryInfo.paymentMethodTable)

/**
 * Gets a [PaymentMethodRecord] from this [Row], reading the data from the passed in [paymentMethodTable].
 *
 * @param queryInfo The object containing the information about the query that produced this row, so that the correct
 * aliased names for tables/fields can be found.
 * @param paymentMethodTable: The instance of the table from which the record is to be extracted.
 */
fun Row.toPaymentMethodRecord(queryInfo: QueryInfo<out Record>,
                              paymentMethodTable: Table<PaymentMethodRecord>)
        : PaymentMethodRecord {

    return PaymentMethodRecord(
            queryInfo.getLong(this, paymentMethodTable, PAYMENT_METHOD.ID),
            queryInfo.getString(this, paymentMethodTable, PAYMENT_METHOD.DESCRIPTION),
            queryInfo.getDouble(this, paymentMethodTable, PAYMENT_METHOD.CHARGE))
}

/**
 * Gets a [PricingDetailsRecord] from this [Row], reading the data from the passed in [queryInfo]'s
 * [primaryTable][QueryInfo.primaryTable].
 *
 * @param queryInfo The object containing the information about the query that produced this row, so that the correct
 * aliased names for tables/fields can be found.
 */
fun Row.toPricingDetailsRecord(queryInfo: PricingDetailsQueryInfo) =
        this.toPricingDetailsRecord(queryInfo, queryInfo.primaryTable)

/**
 * Gets a [PricingDetailsRecord] from this [Row], reading the data from the passed in [pricingDetailsTable].
 *
 * @param queryInfo The object containing the information about the query that produced this row, so that the correct
 * aliased names for tables/fields can be found.
 * @param pricingDetailsTable: The instance of the table from which the record is to be extracted.
 */
fun Row.toPricingDetailsRecord(queryInfo: QueryInfo<out Record>,
                               pricingDetailsTable: Table<PricingDetailsRecord>): PricingDetailsRecord {

    return PricingDetailsRecord(
            queryInfo.getLong(this, pricingDetailsTable, PRICING_DETAILS.ID),
            queryInfo.getString(this, pricingDetailsTable, PRICING_DETAILS.DESCRIPTION),
            queryInfo.getLong(this, pricingDetailsTable, PRICING_DETAILS.VAT_RATE),
            queryInfo.getLong(this, pricingDetailsTable, PRICING_DETAILS.DISCOUNT_RATE),
            queryInfo.getLong(this, pricingDetailsTable, PRICING_DETAILS.PREFERRED_PAYMENT_METHOD))
}

/**
 * Subclass of [QueryInfo] for use by the [PricingDetailsRepository], which, in order to create a [PricingDetails]
 * object, requires data from multiple tables. Specifically, when constructed this object automatically joins to the
 * [VAT_RATE], [DISCOUNT_RATE] and [PAYMENT_METHOD] tables. These are exposed by the [vatRateTable], [discountRateTable]
 * and [paymentMethodTable] properties respectively.
 */
class PricingDetailsQueryInfo(table: Table<PricingDetailsRecord>) : QueryInfo<PricingDetailsRecord>(table) {
    val vatRateTable: Table<VatRateRecord> = addJoinedTable(VAT_RATE, primaryTable, "vat", false)
    val discountRateTable: Table<DiscountRateRecord> = addJoinedTable(DISCOUNT_RATE, primaryTable, "disc", false)
    val paymentMethodTable: Table<PaymentMethodRecord> = addJoinedTable(PAYMENT_METHOD, primaryTable, "paym", false)
}