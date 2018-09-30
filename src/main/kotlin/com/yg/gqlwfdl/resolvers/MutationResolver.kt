package com.yg.gqlwfdl.resolvers

import com.coxautodev.graphql.tools.GraphQLMutationResolver
import com.coxautodev.graphql.tools.GraphQLResolver
import com.yg.gqlwfdl.*
import com.yg.gqlwfdl.dataaccess.DBConfig
import com.yg.gqlwfdl.services.*
import com.yg.gqlwfdl.unitofwork.UnitOfWork
import graphql.schema.DataFetchingEnvironment
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.future.asCompletableFuture
import kotlinx.coroutines.experimental.future.await
import org.springframework.stereotype.Component
import java.time.OffsetDateTime
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.logging.Level
import kotlin.system.measureTimeMillis

@Suppress("unused")
/**
 * Class containing the mutations (e.g. inserts, updates) invoked by GraphQL requests.
 */
@Component
class MutationResolver(
        private val dbConfig: DBConfig,
        private val customerService: CustomerService,
        private val productService: ProductService,
        private val orderService: OrderService) :
        GraphQLMutationResolver {

    private val logger = getLogger()
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
            Mutation<List<Customer>> {

        val customerIds = customersInput.mapNotNull { it.id }

        return object : Mutation<List<Customer>> {
            override fun action(unitOfWork: UnitOfWork): CompletionStage<Void> {
                return customerService.findByIds(customerIds, env.toEntityRequestInfo()).thenAccept { customers ->
                    customers.forEach { customer ->
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
                }
            }

            override fun result(): CompletionStage<List<Customer>> {
                return customerService.findByIds(customerIds)
            }
        }
    }

    fun updateCustomersWithUow(customersInput: List<CustomerInput>, env: DataFetchingEnvironment):
            CompletionStage<List<Customer>> {

        val unitOfWork = env.requestContext.unitOfWork

        return async {
            val customerIds = customersInput.mapNotNull { it.id }
            val customers = customerService.findByIds(customerIds, env.toEntityRequestInfo()).await()

            customers.forEach { customer ->
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

            unitOfWork.complete().await()

            customerService.findByIds(customerIds).await()
        }.asCompletableFuture()
    }

    fun updateCustomer(customerInput: CustomerInput, env: DataFetchingEnvironment): CompletionStage<Customer> {
        return async {
            updateCustomersWithUow(listOf(customerInput), env).await().first()
        }.asCompletableFuture()
    }

    fun createCustomer(customerInput: CustomerInput, env: DataFetchingEnvironment): Mutation<CustomerID> {
        val createCustomers = createCustomers(listOf(customerInput))
        return object : Mutation<CustomerID> {
            override fun action(unitOfWork: UnitOfWork): CompletionStage<*> {
                return createCustomers.action(unitOfWork)
            }

            override fun result(): CompletionStage<CustomerID> {
                return createCustomers.result().thenApply { it.first() }
            }
        }
    }

    fun createCustomers(customersInput: List<CustomerInput>): Mutation<List<CustomerID>> {
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

        return object : Mutation<List<CustomerID>> {
            override fun action(unitOfWork: UnitOfWork): CompletionStage<*> {
                // Manually tracking customer creation here, in actual use this would probably be done by creating the customer
                // through a factory, possibly attached to the unit of work itself.
                customers.map { unitOfWork.trackNew(it) }

                return CompletableFuture.completedFuture(null)
            }

            override fun result(): CompletionStage<List<CustomerID>> {
                val customerIds = customers.map { customer ->
                    customer.id ?: throw NullPointerException("Customer should be persisted and ID set on entity " +
                            "prior to completing the create customer operation")
                }
                return CompletableFuture.completedFuture(customerIds)
            }
        }
    }

    fun deleteCustomers(customerIds: List<CustomerID>): Mutation<List<Boolean>> {
        return object : Mutation<List<Boolean>> {
            var tracking: List<CompletionStage<Int>?>? = null
            override fun action(unitOfWork: UnitOfWork): CompletionStage<*> {
                return async {
                    val customers = customerService.findByIds(customerIds).await()
                    tracking = customerIds.map { customerId ->
                        val customer = customers.find { it.id == customerId }
                        customer?.let { unitOfWork.trackDelete(it) }
                    }
                }.asCompletableFuture()
            }

            override fun result(): CompletionStage<List<Boolean>> {
                return async {
                    tracking!!.map { it?.await() == 1 }
                }.asCompletableFuture()
            }
        }
    }

    fun createOrder(orderInput: OrderInput): Mutation<Order> {
        logger?.log(Level.INFO, "Order creation requested with $orderInput")
        return object : Mutation<Order> {
            private var order: Order? = null

            override fun action(unitOfWork: UnitOfWork): CompletionStage<*> {
                return async {
                    val productIds = orderInput.lines
                            .map { it.product }

                    val products = productService.findByIds(productIds).await()

                    order = orderService.createOrder(orderInput, products, unitOfWork).await()

                    order
                }.asCompletableFuture()
            }

            override fun result(): CompletionStage<Order> {
                return CompletableFuture.completedFuture(order!!)
            }
        }
    }

    fun createOrderController(orderInput: OrderInput): Mutation<Order> {
        getLogger()?.log(Level.INFO, "Order creation requested with $orderInput")

        return object : Mutation<Order> {
            private var order: Order? = null

            override fun action(unitOfWork: UnitOfWork): CompletionStage<*> {
                return async {
                    val productIds = orderInput.lines
                            .map { it.product }

                    val products = productService.findByIds(productIds).await()

                    orderService.createOrder(orderInput, products, unitOfWork)

                    val orderLines = mutableListOf<Order.Line>()

                    order = Order(
                            id = orderInput.id,
                            customerId = orderInput.customer,
                            deliveryAddress = orderInput.deliveryAddress,
                            date = OffsetDateTime.parse(orderInput.date),
                            lines = orderLines
                    ).also { order ->
                        unitOfWork.trackNew(order)

                        orderInput.lines.mapTo(orderLines) { lineInput ->
                            Order.Line(
                                    null,
//                                    product = products.find { lineInput.product == it.id }!!,
                                    product = EntityOrId.Entity(products.find { lineInput.product == it.id }!!),
                                    price = lineInput.price.toDouble(),
//                                    order = EntityOrId.Id(order.id)
                                    order = order.id!!
                            )
                        }.also { orderLines ->
                            orderLines.forEach {
                                unitOfWork.trackNew(it)
                            }
                        }
                    }

                }.asCompletableFuture()
            }

            override fun result(): CompletionStage<Order> {
                return CompletableFuture.completedFuture(order!!)
            }
        }
    }

    fun getNextOrderSequence(): CompletableFuture<OrderID> = orderService.getNextSequence()

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
