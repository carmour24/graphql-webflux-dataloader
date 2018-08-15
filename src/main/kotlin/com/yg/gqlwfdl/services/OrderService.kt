package com.yg.gqlwfdl.services

import com.yg.gqlwfdl.dataaccess.EntityRequestInfo
import com.yg.gqlwfdl.dataaccess.OrderRepository
import org.springframework.stereotype.Service
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
}

/**
 * Concrete implementation of [see OrderService]
 */
@Service
class DefaultOrderService(private val orderRepository: OrderRepository) : OrderService {

    override fun findAll(requestInfo: EntityRequestInfo?) = orderRepository.findAll(requestInfo)

    override fun findByIds(ids: List<Long>, requestInfo: EntityRequestInfo?) =
            orderRepository.findByIds(ids, requestInfo)

    override fun findByCustomerIds(customerIds: List<Long>, requestInfo: EntityRequestInfo?) =
            orderRepository.findByCustomerIds(customerIds, requestInfo)
}