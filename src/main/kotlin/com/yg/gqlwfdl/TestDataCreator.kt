package com.yg.gqlwfdl

import com.yg.gqlwfdl.dataaccess.DBConfig
import com.yg.gqlwfdl.services.*
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.Statement
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.util.*

private val allTables = listOf("customer", "company", "company_partnership", "vat_rate", "discount_rate",
        "payment_method", "pricing_details", "product", "order", "order_line")

private val allSequences = allTables.map { "${it}_id_seq" }

private const val COMPANY_COUNT = 20
private const val CUSTOMERS_PER_COMPANY = 10
private const val PRICING_DETAILS_COUNT = 4
private const val PRODUCT_COUNT = 1000
private val ORDERS_PER_CUSTOMERS = listOf(0, 1, 2, 3, 5, 10)
private const val ADDRESSES_PER_CUSTOMER = 3
private val LINES_PER_ORDER = listOf(1, 2, 3, 4, 5, 10)

/**
 * Used to create test data.
 */
class TestDataCreator(private val dbConfig: DBConfig) {
    fun execute(): Map<String, Int> {

        DriverManager.getConnection(dbConfig.url, dbConfig.username, dbConfig.password).use { connection ->
            connection.createStatement().use { statement ->
                deleteExistingData(statement)
                resetSequences(statement)
                createTestData(statement)

                fun getRecordCount(tableName: String) = statement.executeQuery("""select count(*) from "$tableName"""").use {
                    it.next()
                    it.getInt(1)
                }

                return allTables.map { Pair(it, getRecordCount(it)) }.toMap()
            }
        }
    }

    private fun createTestData(statement: Statement) {
        val vatRates = createVatRates(statement)
        val discountRates = createDiscountRates(statement)
        val paymentMethods = createPaymentMethods(statement)
        val pricingDetails = createPricingDetails(statement, vatRates, discountRates, paymentMethods)
        val companies = createCompanies(statement, pricingDetails)
        val customers = createCustomers(statement, companies, pricingDetails)
        setOutOfOfficeDelegates(statement, customers)
        setPrimaryContacts(statement, companies, customers)
        createCompanyPartnerships(statement, companies)
        val products = createProducts(statement, companies)
        val orderIds = createOrders(statement, customers)
        createOrderLines(statement, orderIds, products)
    }

    private fun createOrderLines(statement: Statement, orderIds: List<Long>, products: List<Product>) {
        orderIds.forEach { orderId ->
            products.randomItems(LINES_PER_ORDER.randomItem()).forEach { product ->
                statement.execute("""
                        |insert into order_line ("order", product, price)
                        |values ($orderId, ${product.id}, ${getRandomPrice()});
                    """.trimMargin())
            }
        }
    }

    private fun createOrders(statement: Statement, customers: List<Customer>): List<Long> {
        val secondsInAYear = 365 * 24 * 60 * 60

        customers.forEach { customer ->
            val orderCount = ORDERS_PER_CUSTOMERS.randomItem()
            if (orderCount > 0) {
                // Generate addresses for the customer, and choose a random one for each order.
                val addresses = (1..ADDRESSES_PER_CUSTOMER).map {
                    "Address $it for ${customer.firstName} ${customer.lastName}"
                }

                // Create the orders for this customer.
                for (i in 1..orderCount) {
                    val date = OffsetDateTime.now().minusSeconds((0..secondsInAYear).randomItem().toLong())
                    val dateString = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(date)
                    statement.execute("""
                        |insert into "order" (customer, date, delivery_address)
                        |values (${customer.id}, '$dateString', '${addresses.randomItem()}');
                    """.trimMargin())
                }
            }
        }

        return listFromQuery(statement, """select id from "order" order by id""") { it.getLong("id") }
    }

    private fun getRandomPrice() = (99..9999).randomItem().toDouble() / 100

    private fun createProducts(statement: Statement, companies: List<Company>): List<Product> {
        for (i in 1..PRODUCT_COUNT) {
            statement.addBatch("""
                |insert into product (description, price, company)
                |values ('Product $i', ${getRandomPrice()}, ${companies.randomItem().id});
            """.trimMargin())
        }
        statement.executeBatch()

        return listFromQuery(statement, "select id, description, price, company from product order by id") {
            Product(it.getLong("id"), it.getString("description"), it.getDouble("price"), it.getLong("company"))
        }
    }

    private fun createVatRates(statement: Statement): List<VatRate> {
        mapOf(Pair("Standard", 20.0), Pair("Reduced", 5.0), Pair("Zero", 0.0)).forEach {
            statement.addBatch("insert into vat_rate (description, value) values ('${it.key}', ${it.value});")
        }
        statement.executeBatch()

        return listFromQuery(statement, "select id, description, value from vat_rate order by id") {
            VatRate(it.getLong("id"), it.getString("description"), it.getDouble("value"))
        }
    }

    private fun createDiscountRates(statement: Statement): List<DiscountRate> {
        mapOf(Pair("None", 0.0), Pair("Cheap", 5.0), Pair("Cheapest", 10.0)).forEach {
            statement.addBatch("insert into discount_rate (description, value) values ('${it.key}', ${it.value});")
        }
        statement.executeBatch()

        return listFromQuery(statement, "select id, description, value from discount_rate order by id") {
            DiscountRate(it.getLong("id"), it.getString("description"), it.getDouble("value"))
        }
    }

    private fun createPaymentMethods(statement: Statement): List<PaymentMethod> {
        mapOf(Pair("Cash", 0.0), Pair("Cheque", 2.5), Pair("Card", 1.5)).forEach {
            statement.addBatch("insert into payment_method (description, charge) values ('${it.key}', ${it.value});")
        }
        statement.executeBatch()

        return listFromQuery(statement, "select id, description, charge from payment_method order by id") {
            PaymentMethod(it.getLong("id"), it.getString("description"), it.getDouble("charge"))
        }
    }

    private fun createPricingDetails(statement: Statement, vatRates: List<VatRate>, discountRates: List<DiscountRate>,
                                     paymentMethods: List<PaymentMethod>)
            : List<PricingDetails> {

        (1..PRICING_DETAILS_COUNT).forEach {
            statement.addBatch("""
                | insert into pricing_details (description, vat_rate, discount_rate, preferred_payment_method) values (
                |   'PricingDetails-$it',
                |   ${vatRates.randomItem().id},
                |   ${discountRates.randomItem().id},
                |   ${paymentMethods.randomItem().id}
                | );
            """.trimMargin())
        }
        statement.executeBatch()

        return listFromQuery(statement,
                "select id, description, vat_rate, discount_rate, preferred_payment_method from pricing_details order by id") {
            PricingDetails(it.getLong("id"), it.getString("description"),
                    vatRates.first { vatRate -> vatRate.id == it.getLong("vat_rate") },
                    discountRates.first { discountRate -> discountRate.id == it.getLong("discount_rate") },
                    paymentMethods.first { paymentMethod -> paymentMethod.id == it.getLong("preferred_payment_method") })
        }
    }

    private fun createCompanyPartnerships(statement: Statement, companies: List<Company>) {
        // Create partnerships between each company and the next two companies
        fun getCreatePartnershipSql(companyA: Company, companyB: Company) =
                "insert into company_partnership (company_a, company_b) values (${companyA.id}, ${companyB.id});"

        for (i in 0..companies.size - 3) {
            statement.addBatch(getCreatePartnershipSql(companies[i], companies[i + 1]))
            statement.addBatch(getCreatePartnershipSql(companies[i], companies[i + 2]))
        }
        statement.executeBatch()
    }

    private fun setPrimaryContacts(statement: Statement, companies: List<Company>, customers: List<Customer>) {
        // Run through all existing companies, and for each one set their primary contact.
        // There's a 50% chance that this will be null.  If non-null, it will be a randomly chosen user from that company.
        val customersByCompany = customers.groupBy { it.companyId }
        val random = Random()

        fun Company.setPrimaryContact() {
            // Get a list of the people in this company.
            customersByCompany[this.id]?.let {
                // Choose a random customer from this list and use them as the primary contact.
                this.primaryContact = it.randomItem().id
                statement.addBatch(
                        "update company set primary_contact = ${this.primaryContact} where id = ${this.id};")
            }
        }

        companies.filter { random.nextBoolean() }.forEach { it.setPrimaryContact() }

        // Make sure that there is at least one company with a primary contact
        if (companies.all { it.primaryContact == null })
            companies.randomItem().setPrimaryContact()

        statement.executeBatch()
    }

    private fun setOutOfOfficeDelegates(statement: Statement, customers: List<Customer>) {
        // Run through all existing customers, and for each one set their out-of-office delegate.
        // There's a 50% chance that this will be null.  If non-null, it will be a randomly chosen user from the same company.
        val customersByCompany = customers.groupBy { it.companyId }
        val random = Random()

        fun Customer.setOutOfOfficeDelegate() {
            // Get a list of the other people in the same company as this customer, excluding the customer themselves.
            customersByCompany[this.companyId]?.minusElement(this)?.let {
                // Choose a random customer from this list and use them as the delegate.
                this.outOfOfficeDelegate = it.randomItem().id
                statement.addBatch(
                        "update customer set out_of_office_delegate = ${this.outOfOfficeDelegate} where id = ${this.id};")
            }
        }

        customers.filter { random.nextBoolean() }.forEach { it.setOutOfOfficeDelegate() }

        // Make sure that there is at least one customer with an out-of-office delegate
        if (customers.all { it.outOfOfficeDelegate == null })
            customers.randomItem().setOutOfOfficeDelegate()

        statement.executeBatch()
    }

    private fun createCustomers(statement: Statement, companies: List<Company>, pricingDetails: List<PricingDetails>)
            : List<Customer> {

        companies.forEach { company: Company ->
            (1..CUSTOMERS_PER_COMPANY).forEach { userNumber: Int ->
                statement.addBatch("""
                    | insert into customer (first_name, last_name, company, pricing_details) values (
                    | 'User-$userNumber', 'From-${company.name}', ${company.id}, ${pricingDetails.randomItem().id});
                """.trimMargin())
            }
        }
        statement.executeBatch()

        return listFromQuery(statement,
                "select id, first_name, last_name, company, pricing_details from customer order by id") {
            Customer(it.getLong("id"), it.getString("first_name"), it.getString("last_name"),
                    it.getLong("company"), it.getLong("pricing_details"))
        }
    }

    private fun createCompanies(statement: Statement, pricingDetails: List<PricingDetails>): List<Company> {
        (1..COMPANY_COUNT).forEach {
            statement.addBatch("""
                | insert into company (name, address, pricing_details)
                | values ('Company-$it', '$it Street, $it Town', ${pricingDetails.randomItem().id});
            """.trimMargin())
        }
        statement.executeBatch()

        return listFromQuery(statement, "select id, name, address, pricing_details from company order by id") {
            Company(it.getLong("id"), it.getString("name"), it.getString("address"), it.getLong("pricing_details"))
        }
    }

    private fun <T : Any> listFromQuery(statement: Statement, sql: String, extractor: (ResultSet) -> T): List<T> {
        return statement.executeQuery(sql).use {
            // From https://stackoverflow.com/questions/44315985/producing-a-list-from-a-resultset
            generateSequence {
                if (it.next()) extractor(it) else null
            }.toList()
        }
    }

    private fun deleteExistingData(statement: Statement) {
        allTables.forEach { statement.execute("""delete from "$it";""") }
    }

    private fun resetSequences(statement: Statement) {
        allSequences.forEach { statement.execute("""alter sequence "$it" restart;""") }
    }
}

private fun <T> List<T>.randomItem(): T = this[Random().nextInt(this.size)]

private fun ClosedRange<Int>.randomItem() = Random().nextInt((endInclusive + 1) - start) + start

private fun <T> List<T>.randomItems(count: Int): List<T> =
        this.toMutableList().let { (1..count).map { _ -> it.removeAt((0 until it.size).randomItem()) } }