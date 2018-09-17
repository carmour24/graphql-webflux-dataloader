package com.yg.gqlwfdl.resolvers

import com.coxautodev.graphql.tools.GraphQLMutationResolver
import com.opidis.unitofwork.data.DefaultEntityTrackingUnitOfWork
import com.yg.gqlwfdl.TestDataCreator
import com.yg.gqlwfdl.dataaccess.DBConfig
import com.yg.gqlwfdl.getLogger
import com.yg.gqlwfdl.requestContext
import com.yg.gqlwfdl.services.Customer
import com.yg.gqlwfdl.services.CustomerID
import com.yg.gqlwfdl.services.CustomerService
import com.yg.gqlwfdl.unitofwork.QueryAction
import com.yg.gqlwfdl.unitofwork.QueryCoordinator
import com.yg.gqlwfdl.unitofwork.UnitOfWork
import graphql.schema.DataFetchingEnvironment
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.awaitAll
import kotlinx.coroutines.experimental.future.asCompletableFuture
import kotlinx.coroutines.experimental.future.await
import kotlinx.coroutines.experimental.future.toCompletableFuture
import reactor.core.publisher.Mono
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletableFuture.allOf
import java.util.concurrent.CompletionStage
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.system.measureTimeMillis

@Suppress("unused")
/**
 * Class containing the mutations (e.g. inserts, updates) invoked by GraphQL requests.
 */
class Mutation(private val dbConfig: DBConfig, private val customerService: CustomerService) :
        GraphQLMutationResolver {

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

    fun updateCustomers(customersInput: List<CustomerInput>, env: DataFetchingEnvironment):
            CompletionStage<List<Customer>> {

        val unitOfWork = env.requestContext.unitOfWork

        val customerIds = customersInput.mapNotNull { it.id }
        val customers = customerService.findByIds(customerIds)

        return customers.thenApply { customers ->
            customers.forEach { customer ->
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

//            env.requestContext.unitOfWork.complete().thenCompose {
//                this.getLogger()?.log(Level.FINE, "finding")
//                customerService.findByIds(customerIds)
//            }
            customers
        }
    }

//    fun updateCustomers(customersInput: List<CustomerInput>, env: DataFetchingEnvironment):
//            CompletionStage<List<Customer>> {
//
//        val unitOfWork = env.requestContext.unitOfWork
//
//        return async {
//            val customerIds = customersInput.mapNotNull { it.id }
//            val customers = customerService.findByIds(customerIds).await()
//
//            customers.forEach { customer ->
//                unitOfWork.trackEntityForChanges(customer)
//                val customerInput = customersInput.find { it.id == customer.id }
//
//                if (customerInput != null) {
//                    with(customer) {
//                        firstName = customerInput.firstName
//                        lastName = customerInput.lastName
//                        companyId = customerInput.company
//                        pricingDetailsId = customerInput.pricingDetails
//                        outOfOfficeDelegate = customerInput.outOfOfficeDelegate
//                    }
//                }
//            }
//
//            unitOfWork.completionStage.toCompletableFuture().await()
//
//            customerService.findByIds(customerIds).await()
//        }.asCompletableFuture()
//    }

    fun updateCustomer(customerInput: CustomerInput, env: DataFetchingEnvironment): CompletionStage<Customer> {
        return async {
            updateCustomers(listOf(customerInput), env).await().first()
        }.asCompletableFuture()
    }

    fun createCustomer(customerInput: CustomerInput, env: DataFetchingEnvironment): CompletionStage<CustomerID> {
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

        val unitOfWork = env.requestContext.unitOfWork

        // Manually tracking customer creation here, in actual use this would probably be done by creating the customer
        // through a factory, possibly attached to the unit of work itself.
        val tracking = unitOfWork.trackNew(customer)

        return tracking.thenApply {
            customer.id ?: throw NullPointerException("Customer should be persisted and ID set on entity " +
                    "prior to completing the create customer operation")
        }
    }

    fun createCustomers(customersInput: List<CustomerInput>, env: DataFetchingEnvironment): CompletionStage<List<CustomerID>> {
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

        // Manually tracking customer creation here, in actual use this would probably be done by creating the customer
        // through a factory, possibly attached to the unit of work itself.
        val unitOfWork = env.requestContext.unitOfWork
        val tracking = customers.map { unitOfWork.trackNew(it).toCompletableFuture() }

        return allOf(*tracking.toTypedArray()).thenApply {
            customers.map { customer ->
                customer.id ?: throw NullPointerException("Customer should be persisted and ID set on entity " +
                        "prior to completing the create customer operation")
            }
        }
    }

    fun deleteCustomers(customerIds: List<CustomerID>, env: DataFetchingEnvironment): CompletionStage<List<Boolean>> {
        return async {
            val unitOfWork = env.requestContext.unitOfWork

            val customers = customerService.findByIds(customerIds).await()

            val tracking = customers.map { unitOfWork.trackDelete(it) }


            val deleteResults = tracking.map { it.await() == 1 }

            deleteResults
        }.asCompletableFuture()
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
