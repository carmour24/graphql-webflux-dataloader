package com.yg.gqlwfdl.resolvers

import com.coxautodev.graphql.tools.GraphQLMutationResolver
import com.opidis.unitofwork.data.DefaultEntityTrackingUnitOfWork
import com.yg.gqlwfdl.TestDataCreator
import com.yg.gqlwfdl.dataaccess.DBConfig
import com.yg.gqlwfdl.services.Customer
import com.yg.gqlwfdl.services.CustomerID
import com.yg.gqlwfdl.services.CustomerService
import com.yg.gqlwfdl.unitofwork.QueryAction
import com.yg.gqlwfdl.unitofwork.UnitOfWork
import reactor.core.publisher.Mono
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.logging.Level
import java.util.logging.Logger
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

    fun updateCustomers(customersInput: List<CustomerInput>, unitOfWork: UnitOfWork):
            CompletionStage<List<Customer>> {

        val customerIds = customersInput.mapNotNull { it.id }
        return customerService.findByIds(customerIds).thenApply {
            it.forEach { customer ->
                unitOfWork.trackEntityForChanges(customer)
                val customerInput = customersInput.find { it.id == customer.id }

                if (customerInput != null) {
                    with(customer) {
                        firstName = customerInput.firstName
                        lastName = customerInput.lastName
                        companyId = customerInput.company
                        pricingDetailsId = customerInput.pricingDetails
                        outOfOfficeDelegate = customerInput.outOfOfficeDelegate
                    }
                }
            }
        }.thenCompose {
            unitOfWork.complete()

            customerService.findByIds(customerIds)
        }
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

    fun createOrder(order: OrderInput): Long {
        Logger.getLogger(this.javaClass.kotlin.qualifiedName).log(Level.INFO, "$order")

        return 1
    }

    data class CustomerInput(
            val id: Long?,
            val firstName: String,
            val lastName: String,
            val company: Long,
            val pricingDetails: Long,
            val outOfOfficeDelegate: Long?
    )

    data class OrderInput(
            val id: Long?,
            val customer: Long,
            val date: String,
            val deliveryAddress: String,
            val lines: List<OrderLineInput>
    )

    data class OrderLineInput(
            val id: Long?,
            val product: Long,
            val price: Float
    )
}