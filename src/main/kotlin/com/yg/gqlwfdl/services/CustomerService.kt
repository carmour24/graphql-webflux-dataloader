package com.yg.gqlwfdl.services

import com.yg.gqlwfdl.dataaccess.CustomerRepository
import com.yg.gqlwfdl.dataaccess.EntityRequestInfo
import org.springframework.stereotype.Service
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletableFuture.*
import java.util.concurrent.CompletionStage

/**
 * Service for handling functionality related to customers. Communicates with the data access layer to get the data
 * from the database, and exposes it to callers using the domain model objects (specifically, [Customer]). Performs
 * all actions asynchronously.
 */
interface CustomerService {
    /**
     * Returns a [CompletableFuture] which, when completed, will provide a [List] of all [Customer]s.
     *
     * @param requestInfo Information about the request, such as the fields of the entity which were requested by the
     * client, if the call was made from the context of a client request.
     */
    fun findAll(requestInfo: EntityRequestInfo? = null): CompletableFuture<List<Customer>>

    /**
     * Returns a [CompletableFuture] which, when completed, will provide a [List] of all [Customer]s with the passed in
     * IDs.
     *
     * @param requestInfo Information about the request, such as the fields of the entity which were requested by the
     * client, if the call was made from the context of a client request.
     */
    fun findByIds(ids: List<Long>, requestInfo: EntityRequestInfo? = null): CompletableFuture<List<Customer>>

    fun insert(customers: List<Customer>): CompletionStage<List<CustomerID>>

    fun insert(customer: Customer): CompletionStage<CustomerID>

    fun update(customer: Customer): CompletionStage<Customer>

    fun update(customers: List<Customer>): CompletionStage<List<Customer>>
}

/**
 * Concrete implementation of [see CustomerService]
 * TODO: Add error handling on not all entity updates/inserts succeeding as determined possibly by inaccurate counts
 */
@Service
class DefaultCustomerService(private val customerRepository: CustomerRepository)
    : CustomerService {
    override fun update(customer: Customer): CompletionStage<Customer> = customerRepository.update(customer).thenApply { customer }

    override fun update(customers: List<Customer>): CompletionStage<List<Customer>> {
        return customerRepository.update(customers)
                .thenApply {
                    customers
                }
    }

    override fun insert(customer: Customer) = customerRepository.insert(customer)

    override fun insert(customers: List<Customer>): CompletionStage<List<CustomerID>> = customerRepository.insert(customers).thenApply {
        customers.map {
            it.id ?: throw NullPointerException()
        }.toList()
    }

    override fun findAll(requestInfo: EntityRequestInfo?) = customerRepository.findAll(requestInfo)

    override fun findByIds(ids: List<Long>, requestInfo: EntityRequestInfo?) =
            customerRepository.findByIds(ids, requestInfo)
}