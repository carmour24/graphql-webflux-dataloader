package com.yg.gqlwfdl.dataaccess

import com.yg.gqlwfdl.dataaccess.db.Tables.CUSTOMER
import com.yg.gqlwfdl.dataaccess.db.tables.records.CustomerRecord
import com.yg.gqlwfdl.dataaccess.joins.ClientFieldToJoinMapper
import com.yg.gqlwfdl.dataaccess.joins.JoinedRecordToEntityConverterProvider
import com.yg.gqlwfdl.dataaccess.joins.RecordProvider
import com.yg.gqlwfdl.services.Customer
import com.yg.gqlwfdl.services.CustomerID
import io.reactiverse.pgclient.PgPool
import io.reactiverse.pgclient.Row
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.Table
import org.springframework.stereotype.Repository

/**
 * Repository providing access to customer information.
 */
interface CustomerRepository : EntityRepository<Customer, CustomerID>, MutatingRepository<Customer, CustomerID,
        PgClientExecutionInfo>

/**
 * Concrete implementation of [CustomerRepository], which uses a database for its data.
 */
@Repository
class DBCustomerRepository(create: DSLContext,
                           connectionPool: PgPool,
                           recordToEntityConverterProvider: JoinedRecordToEntityConverterProvider,
                           clientFieldToJoinMapper: ClientFieldToJoinMapper,
                           recordProvider: RecordProvider)
    : SingleRowDBEntityRepository<Customer, CustomerID, CustomerRecord, QueryInfo<CustomerRecord>>(
        create, connectionPool, recordToEntityConverterProvider, clientFieldToJoinMapper, recordProvider,
        CUSTOMER, CUSTOMER.ID),
        MutatingRepository<Customer, CustomerID, PgClientExecutionInfo> by DBMutatingEntityRepository(
                create,
                connectionPool,
                table = CUSTOMER
        ),
        CustomerRepository {

    override fun getEntity(queryInfo: QueryInfo<CustomerRecord>, row: Row) = row.toCustomerRecord(queryInfo).toEntity()
}


/**
 * Converts a [CustomerRecord] to its corresponding entity, a [Customer].
 */
fun CustomerRecord.toEntity() =
        Customer(this.id, this.firstName, this.lastName, this.company, this.pricingDetails, this.outOfOfficeDelegate)

/**
 * Gets a [CustomerRecord] from this [Row], reading the data from the passed in [queryInfo]'s
 * [primaryTable][QueryInfo.primaryTable].
 *
 * @param queryInfo The object containing the information about the query that produced this row, so that the correct
 * aliased names for tables/fields can be found.
 */
fun Row.toCustomerRecord(queryInfo: QueryInfo<CustomerRecord>) =
        this.toCustomerRecord(queryInfo, queryInfo.primaryTable)

/**
 * Gets a [CustomerRecord] from this [Row], reading the data from the passed in [customerTable].
 *
 * @param queryInfo The object containing the information about the query that produced this row, so that the correct
 * aliased names for tables/fields can be found.
 * @param customerTable: The instance of the table from which the record is to be extracted.
 */
fun Row.toCustomerRecord(queryInfo: QueryInfo<out Record>,
                         customerTable: Table<CustomerRecord>): CustomerRecord {

    // TODO: replace the below here and in other implementations with something that uses reflection?
    return CustomerRecord(
            queryInfo.getLong(this, customerTable, CUSTOMER.ID),
            queryInfo.getString(this, customerTable, CUSTOMER.FIRST_NAME),
            queryInfo.getString(this, customerTable, CUSTOMER.LAST_NAME),
            queryInfo.getLong(this, customerTable, CUSTOMER.COMPANY),
            queryInfo.getNullableLong(this, customerTable, CUSTOMER.OUT_OF_OFFICE_DELEGATE),
            queryInfo.getLong(this, customerTable, CUSTOMER.PRICING_DETAILS))
}
