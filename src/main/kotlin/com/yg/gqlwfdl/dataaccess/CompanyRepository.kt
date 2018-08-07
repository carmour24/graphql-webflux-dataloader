package com.yg.gqlwfdl.dataaccess

import com.yg.gqlwfdl.dataaccess.db.Tables.COMPANY
import com.yg.gqlwfdl.dataaccess.db.tables.records.CompanyRecord
import com.yg.gqlwfdl.dataaccess.joins.GraphQLFieldToJoinMapper
import com.yg.gqlwfdl.dataaccess.joins.JoinedRecordToEntityConverterProvider
import com.yg.gqlwfdl.dataaccess.joins.RecordProvider
import com.yg.gqlwfdl.services.Company
import io.reactiverse.pgclient.PgPool
import io.reactiverse.pgclient.Row
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.Table
import org.springframework.stereotype.Repository

/**
 * Repository providing access to company information.
 */
interface CompanyRepository : EntityRepository<Company, Long>

/**
 * Concrete implementation of [CompanyRepository], which uses a database for its data.
 */
@Repository
class DBCompanyRepository(create: DSLContext,
                          connectionPool: PgPool,
                          recordToEntityConverterProvider: JoinedRecordToEntityConverterProvider,
                          graphQLFieldToJoinMapper: GraphQLFieldToJoinMapper,
                          recordProvider: RecordProvider)
    : DBEntityRepository<Company, Long, CompanyRecord, QueryInfo<CompanyRecord>>(
        create, connectionPool, recordToEntityConverterProvider, graphQLFieldToJoinMapper, recordProvider,
        COMPANY, COMPANY.ID),
        CompanyRepository {

    override fun getRecord(queryInfo: QueryInfo<CompanyRecord>, row: Row) = row.toCompanyRecord(queryInfo)

    override fun getEntity(queryInfo: QueryInfo<CompanyRecord>, row: Row) = getRecord(queryInfo, row).toEntity()
}

/**
 * Converts a [CompanyRecord] to its corresponding entity, a [Company].
 */
fun CompanyRecord.toEntity() = Company(this.id, this.name, this.address, this.pricingDetails, this.primaryContact)

/**
 * Gets a [CompanyRecord] from this [Row], reading the data from the passed in [queryInfo]'s
 * [primaryTable][QueryInfo.primaryTable].
 *
 * @param queryInfo The object containing the information about the query that produced this row, so that the correct
 * aliased names for tables/fields can be found.
 */
fun Row.toCompanyRecord(queryInfo: QueryInfo<CompanyRecord>) =
        this.toCompanyRecord(queryInfo, queryInfo.primaryTable)

/**
 * Gets a [CompanyRecord] from this [Row], reading the data from the passed in [companyTable].
 *
 * @param queryInfo The object containing the information about the query that produced this row, so that the correct
 * aliased names for tables/fields can be found.
 * @param companyTable: The instance of the table from which the record is to be extracted.
 */
fun Row.toCompanyRecord(queryInfo: QueryInfo<out Record>,
                        companyTable: Table<CompanyRecord>): CompanyRecord {

    return CompanyRecord(
            queryInfo.getLong(this, companyTable, COMPANY.ID),
            queryInfo.getString(this, companyTable, COMPANY.NAME),
            queryInfo.getString(this, companyTable, COMPANY.ADDRESS),
            queryInfo.getNullableLong(this, companyTable, COMPANY.PRIMARY_CONTACT),
            queryInfo.getLong(this, companyTable, COMPANY.PRICING_DETAILS))
}