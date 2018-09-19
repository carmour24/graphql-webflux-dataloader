package com.yg.gqlwfdl.services

import com.yg.gqlwfdl.dataaccess.EntityRequestInfo
import com.yg.gqlwfdl.dataaccess.OrderRepository
import com.yg.gqlwfdl.dataaccess.db.Sequences
import com.yg.gqlwfdl.resolvers.MutationResolver
import com.yg.gqlwfdl.unitofwork.UnitOfWork
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.future.asCompletableFuture
import org.springframework.stereotype.Service
import java.time.OffsetDateTime
import java.util.concurrent.CompletableFuture

/**
 * Service for handling functionality related to order. Communicates with the data access layer to get the data
 * from the database, and exposes it to callers using the domain model objects (specifically, [Order]). Performs
 * all actions asynchronously.
 */
interface OrderService {
    /**
     * Returns a [CompletableFuture] which, when completed, will provide a [List] of all [Order] objects.
     *
     * @param requestInfo Information about the request, such as the fields of the entity which were requested by the
     * client, if the call was made from the context of a client request.
     */
    fun findAll(requestInfo: EntityRequestInfo? = null): CompletableFuture<List<Order>>

    /**
     * Returns a [CompletableFuture] which, when completed, will provide a [List] of all [Order] objects with the
     * passed in IDs.
     *
     * @param requestInfo Information about the request, such as the fields of the entity which were requested by the
     * client, if the call was made from the context of a client request.
     */
    fun findByIds(ids: List<Long>, requestInfo: EntityRequestInfo? = null): CompletableFuture<List<Order>>

    /**
     * Returns a [CompletableFuture] which, when completed, will provide a [List] of all [Order] objects belonging
     * to [Customer]s with the passed in [customerIds].
     *
     * @param requestInfo Information about the request, such as the fields of the entity which were requested by the
     * client, if the call was made from the context of a client request.
     */
    fun findByCustomerIds(customerIds: List<Long>, requestInfo: EntityRequestInfo? = null):
            CompletableFuture<List<Order>>

    fun createOrder(orderInput: MutationResolver.OrderInput, products: List<Product>, unitOfWork: UnitOfWork) :
            CompletableFuture<Order>
}

/*
 * Concrete implementation of [see OrderService]
 */
@Service
class DefaultOrderService(private val orderRepository: OrderRepository) : OrderService {
    override fun createOrder(orderInput: MutationResolver.OrderInput, products: List<Product>, unitOfWork: UnitOfWork):
            CompletableFuture<Order> {
        val productIds = orderInput.lines
                .map { it.product }

        return async {
            val orderLines = mutableListOf<Order.Line>()


            Order(
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
                            product = products.find { lineInput.product == it.id }!!,
                            price = lineInput.price.toDouble(),
                            orderID = orderInput.id!!
                    )
                }.also { orderLines ->
                    orderLines.forEach {
                        unitOfWork.trackNew(it)
                    }
                }
            }
        }.asCompletableFuture()
    }

    override fun findAll(requestInfo: EntityRequestInfo?) = orderRepository.findAll(requestInfo)

    override fun findByIds(ids: List<Long>, requestInfo: EntityRequestInfo?) =
            orderRepository.findByIds(ids, requestInfo)

    override fun findByCustomerIds(customerIds: List<Long>, requestInfo: EntityRequestInfo?) =
            orderRepository.findByCustomerIds(customerIds, requestInfo)
}