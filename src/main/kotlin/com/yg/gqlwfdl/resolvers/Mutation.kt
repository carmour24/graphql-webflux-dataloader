package com.yg.gqlwfdl.resolvers

import com.coxautodev.graphql.tools.GraphQLMutationResolver
import com.yg.gqlwfdl.TestDataCreator
import com.yg.gqlwfdl.dataaccess.DBConfig
import com.yg.gqlwfdl.services.Customer
import com.yg.gqlwfdl.services.CustomerID
import com.yg.gqlwfdl.services.CustomerService
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import kotlin.system.measureTimeMillis

@Suppress("unused")
/**
 * Class containing the mutations (e.g. inserts, updates) invoked by GraphQL requests.
 */
class Mutation(private val dbConfig: DBConfig, private val customerService: CustomerService) : GraphQLMutationResolver {

    /**
     * Deletes all existing data and populates the database with a bunch of randomly generated test data.
     */
    fun createTestData(): String {
        val recordCounts = mutableMapOf<String, Int>()
        val totalTime = measureTimeMillis { recordCounts.putAll(TestDataCreator(dbConfig).execute()) }
        val stringBuilder = StringBuilder("Test data successfully created in ${totalTime}ms")
        recordCounts.toSortedMap(String.CASE_INSENSITIVE_ORDER).forEach {
            stringBuilder.append(" ...").append(it.key).append(": ").append(it.value)
        }
        return stringBuilder.toString()
    }

    fun updateCustomers(customersInput: List<CustomerInput>): CompletionStage<List<Customer>> {
        val customers = customersInput.map { customerInput ->
            with(customerInput) {
                Customer(
                        id = id,
                        firstName = firstName,
                        lastName = lastName,
                        companyId = company,
                        pricingDetailsId = pricingDetails,
                        outOfOfficeDelegate = outOfOfficeDelegate
                )
            }
        }
        return customerService.update(customers)
    }

    fun updateCustomer(customerInput: CustomerInput): CompletionStage<Customer> {
        val customer = with(customerInput) {
            Customer(
                    id = id,
                    firstName = firstName,
                    lastName = lastName,
                    companyId = company,
                    pricingDetailsId = pricingDetails,
                    outOfOfficeDelegate = outOfOfficeDelegate
            )
        }
        return customerService.update(customer)
    }

    fun createCustomer(customerInput: CustomerInput): CompletionStage<CustomerID> {
        val customer = with(customerInput) {
            Customer(
                    id = null,
                    firstName = firstName,
                    lastName = lastName,
                    companyId = company,
                    pricingDetailsId = pricingDetails,
                    outOfOfficeDelegate = outOfOfficeDelegate
            )
        }
        return customerService.insert(customer)
    }

    fun createCustomers(customersInput: List<CustomerInput>): CompletionStage<List<CustomerID>> {
        val customers = customersInput.map {
            with(it) {
                Customer(
                        id = id,
                        firstName = firstName,
                        lastName = lastName,
                        companyId = company,
                        pricingDetailsId = pricingDetails,
                        outOfOfficeDelegate = outOfOfficeDelegate
                )
            }
        }

        return customerService.insert(customers)
    }

    data class CustomerInput(
            val id: Long?,
            val firstName: String,
            val lastName: String,
            val company: Long,
            val pricingDetails: Long,
            val outOfOfficeDelegate: Long?
    )
}