package com.yg.gqlwfdl.dataaccess

import com.yg.gqlwfdl.dataaccess.db.Tables.COMPANY
import com.yg.gqlwfdl.dataaccess.db.Tables.COMPANY_PARTNERSHIP
import com.yg.gqlwfdl.dataaccess.db.tables.records.CompanyPartnershipRecord
import com.yg.gqlwfdl.dataaccess.db.tables.records.CompanyRecord
import com.yg.gqlwfdl.dataaccess.joins.*
import com.yg.gqlwfdl.services.CompanyPartnership
import io.reactiverse.pgclient.PgPool
import io.reactiverse.pgclient.Row
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.Table
import org.jooq.TableField
import org.springframework.stereotype.Repository

/**
 * Repository providing access to company partnership information.
 */
interface CompanyPartnershipRepository : EntityRepository<CompanyPartnership, Long>

/**
 * Concrete implementation of [CompanyPartnershipRepository], which uses a database for its data.
 */
@Repository
class DBCompanyPartnershipRepository(create: DSLContext,
                                     connectionPool: PgPool,
                                     recordToEntityConverterProvider: JoinedRecordToEntityConverterProvider,
                                     clientFieldToJoinMapper: ClientFieldToJoinMapper,
                                     recordProvider: RecordProvider)
    : DBEntityRepository<CompanyPartnership, Long, CompanyPartnershipRecord, CompanyPartnershipQueryInfo>(
        create, connectionPool, recordToEntityConverterProvider, clientFieldToJoinMapper, recordProvider,
        COMPANY_PARTNERSHIP, COMPANY_PARTNERSHIP.ID),
        CompanyPartnershipRepository {

    override fun getQueryInfo(table: Table<CompanyPartnershipRecord>) = CompanyPartnershipQueryInfo(table)

    override fun getRecord(queryInfo: CompanyPartnershipQueryInfo, row: Row) = row.toCompanyPartnershipRecord(queryInfo)

    override fun getEntity(queryInfo: CompanyPartnershipQueryInfo, row: Row): CompanyPartnership {
        return CompanyPartnershipRecords(
                row.toCompanyPartnershipRecord(queryInfo),
                row.toCompanyRecord(queryInfo, queryInfo.companyATable),
                row.toCompanyRecord(queryInfo, queryInfo.companyBTable)
        ).toEntity()
    }

    // Ignore unchecked casts here for the places we ask for a field's instance from a table, as we know the returned
    // value will be a TableField
    @Suppress("UNCHECKED_CAST")
    override fun getDefaultJoins(queryInfo: CompanyPartnershipQueryInfo)
            : List<JoinInstance<out Any, CompanyPartnershipRecord, out Record>> {

        return listOf(
                JoinInstance<Long, CompanyPartnershipRecord, CompanyRecord>(
                        COMPANY_PARTNERSHIP_COMPANY_A,
                        queryInfo.primaryTable.field(COMPANY_PARTNERSHIP.COMPANY_A) as TableField<CompanyPartnershipRecord, Long>,
                        queryInfo.companyATable.field(COMPANY.ID) as TableField<CompanyRecord, Long>),
                JoinInstance<Long, CompanyPartnershipRecord, CompanyRecord>(
                        COMPANY_PARTNERSHIP_COMPANY_B,
                        queryInfo.primaryTable.field(COMPANY_PARTNERSHIP.COMPANY_B) as TableField<CompanyPartnershipRecord, Long>,
                        queryInfo.companyBTable.field(COMPANY.ID) as TableField<CompanyRecord, Long>))
    }
}

/**
 * A class aggregating all the different types of records that contain the data required to construct a [CompanyPartnership]
 * object.
 */
data class CompanyPartnershipRecords(val companyPartnershipRecord: CompanyPartnershipRecord,
                                     val companyARecord: CompanyRecord,
                                     val companyBRecord: CompanyRecord) {
    /**
     * Converts this [CompanyPartnershipRecords] object to its corresponding entity, a [CompanyPartnership] object.
     */
    fun toEntity() = CompanyPartnership(companyPartnershipRecord.id, companyARecord.toEntity(), companyBRecord.toEntity())
}

/**
 * Gets a [CompanyPartnershipRecord] from this [Row], reading the data from the passed in [queryInfo]'s
 * [primaryTable][QueryInfo.primaryTable].
 *
 * @param queryInfo The object containing the information about the query that produced this row, so that the correct
 * aliased names for tables/fields can be found.
 */
fun Row.toCompanyPartnershipRecord(queryInfo: CompanyPartnershipQueryInfo) =
        this.toCompanyPartnershipRecord(queryInfo, queryInfo.primaryTable)

/**
 * Gets a [CompanyPartnershipRecord] from this [Row], reading the data from the passed in [companyPartnershipTable].
 *
 * @param queryInfo The object containing the information about the query that produced this row, so that the correct
 * aliased names for tables/fields can be found.
 * @param companyPartnershipTable: The instance of the table from which the record is to be extracted.
 */
fun Row.toCompanyPartnershipRecord(queryInfo: QueryInfo<out Record>,
                                   companyPartnershipTable: Table<CompanyPartnershipRecord>)
        : CompanyPartnershipRecord {

    return CompanyPartnershipRecord(
            queryInfo.getLong(this, companyPartnershipTable, COMPANY_PARTNERSHIP.ID),
            queryInfo.getLong(this, companyPartnershipTable, COMPANY_PARTNERSHIP.COMPANY_A),
            queryInfo.getLong(this, companyPartnershipTable, COMPANY_PARTNERSHIP.COMPANY_B))
}

/**
 * Subclass of [QueryInfo] for use by the [CompanyPartnershipRepository], which, in order to create a [CompanyPartnership]
 * object, requires data from multiple tables. Specifically, when constructed this object automatically joins two instances
 * of the [COMPANY] table, one for "Company A" in the partnership, one for "Company B". These are exposed by the
 * [companyATable] and [companyBTable] properties respectively.
 */
class CompanyPartnershipQueryInfo(table: Table<CompanyPartnershipRecord>)
    : QueryInfo<CompanyPartnershipRecord>(table) {

    val companyATable: Table<CompanyRecord> = addJoinedTable(COMPANY, primaryTable, "compa", false)
    val companyBTable: Table<CompanyRecord> = addJoinedTable(COMPANY, primaryTable, "compb", false)
}