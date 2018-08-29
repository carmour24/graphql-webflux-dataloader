package com.yg.gqlwfdl

import com.yg.gqlwfdl.dataaccess.db.Tables.CUSTOMER
import com.yg.gqlwfdl.dataaccess.db.tables.records.CustomerRecord
import com.yg.gqlwfdl.dataaccess.getSqlWithNumberedParams
import com.yg.gqlwfdl.dataaccess.replaceNamedSqlParamsWithNumberedParams
import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.jooq.impl.DSL
import org.jooq.tools.jdbc.MockConnection
import org.jooq.tools.jdbc.MockDataProvider
import org.jooq.tools.jdbc.MockResult
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.sql.Connection

internal class JooqQueryExtensionsTest {
    private var connection: Connection? = null
    private var dslContext: DSLContext? = null
    private var batchSql: Array<String> = emptyArray()


    @BeforeEach
    fun setUp() {

        val mockDataProvider = MockDataProvider {
            batchSql = it.batchSQL()
            println(batchSql.joinToString(separator = "\n"))
            arrayOf(
                    MockResult(batchSql.size)
            )
        }

        connection = MockConnection(mockDataProvider)
        dslContext = DSL.using(connection, SQLDialect.POSTGRES_10)
    }

    @Test
    fun replaceNamedSqlParamsWithNumberedParams() {
        val sql = "insert into table1 (n1, n2, n3 ,n4, n5) values (:1, :2, :3, :4, :5) (:6, :7, :8, :9, :10)" +
                "returning table1.id"

        val sqlWithReplacedParams = replaceNamedSqlParamsWithNumberedParams(sql, 5)

        val regexForMatchingNumberedParam = Regex("\\$\\d+", option = RegexOption.MULTILINE)
        val allMatches = regexForMatchingNumberedParam.findAll(sqlWithReplacedParams)

        assert(allMatches.count { it.groups[0]!!.value == "$1" } == 2)
        assert(allMatches.count { it.groups[0]!!.value == "$2" } == 2)
        assert(allMatches.count { it.groups[0]!!.value == "$3" } == 2)
        assert(allMatches.count { it.groups[0]!!.value == "$4" } == 2)
        assert(allMatches.count { it.groups[0]!!.value == "$5" } == 2)
        // Only values between 1 and 5, matching the above query params should be in the substituted sql string
        assert(!allMatches.any {it.groups[0]!!.value == ("0")})
        assert(!allMatches.any {it.groups[0]!!.value == ("6")})
    }

    @Test
    fun shouldReplaceColonWithDollarForNumberedParameters() {
        val record1 = CustomerRecord()
        record1.id = 11
        record1.firstName = "Chris"
        record1.lastName = "Armour"

        val record2 = CustomerRecord()
        record2.id = 21
        record2.firstName = "Yoni"
        record2.lastName = "Gibbs"

        val record3 = CustomerRecord()
        record3.id = 31
        record3.firstName = "Gordon"
        record3.lastName = "Stewart"

        val record4 = CustomerRecord()
        record4.id = 41
        record4.firstName = "Jason"
        record4.lastName = "Irwin"

        val record5 = CustomerRecord()
        record5.id = 51
        record5.firstName = "Gordon"
        record5.lastName = "Stewart"

        val record6 = CustomerRecord()
        record6.id = 61
        record6.firstName = "Jason"
        record6.lastName = "Irwin"

        val query = dslContext!!.insertInto(CUSTOMER).columns(CUSTOMER.FIRST_NAME, CUSTOMER.LAST_NAME)
                .values(record1.firstName, record1.lastName)
                .values(record2.firstName, record2.lastName)
                .values(record3.firstName, record3.lastName)
                .values(record4.firstName, record4.lastName)
                .values(record5.firstName, record5.lastName)
                .values(record6.firstName, record6.lastName)

        val sql = query.getSqlWithNumberedParams(2)

        assert(sql.contains("$")) { "SQL should have \$n param numbering" }
        assert(!sql.contains(":")) { "SQL should not have :n param numbering" }
    }
}
